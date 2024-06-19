// gather NYPD Officer Profile data
// https://nypdonline.org/link/2

import { promises as fs } from 'fs'
import fetch from 'node-fetch'
import { Scheduler } from 'async-scheduler'

const scheduler = new Scheduler(20)

const reportList = {
  summary:        'https://nypdonline.org/api/reports/c211f8e0-625b-478a-9b68-ceef86098a67/data',
  ranks:          'https://nypdonline.org/api/reports/46bdf450-8956-492f-9a13-ea0c02280806/data',
  documents:      'https://nypdonline.org/api/reports/c13c7baf-77e0-456a-a9f3-945e72a0d26d/data',
  discipline:     'https://nypdonline.org/api/reports/e59337f8-014e-46a0-af99-7b6a3ef4dfa3/data',
  charges:        'https://nypdonline.org/api/reports/d860ce1f-7c6f-48dc-bce6-d7754c56e5cf/data',
  allegations:    'https://nypdonline.org/api/reports/d860ce1f-7c6f-48dc-bce6-d7754c56e5cf/data',
  arrests:        'https://nypdonline.org/api/reports/b9f649fc-b33f-47b2-9d7e-a7760090ae39/data',
  awards:         'https://nypdonline.org/api/reports/916ef68f-3d47-46be-8e8a-79567a3e61f9/data',
  training:       'https://nypdonline.org/api/reports/fb985b1d-96e7-43fe-896a-d9f926ae05b6/data',
  trialDecisions: 'https://nypdonline.org/api/reports/ed551b4a-cd5c-4d8e-bcb9-a0478b4c5dea/data',
}

let lettersRetry = new Map()
let officersRetry = new Map()
const TOKEN_RETRIES = 5

let headers = {
  'Accept': 'application/json, text/plain, */*',
  'Content-Type': 'application/json;charset=UTF-8',
  'Accept-Encoding': 'gzip, deflate, br',
  'Accept-Language': 'en-US,en;q=0.9',
  'Referer': 'https://nypdonline.org/link/2',
  'Sec-Fetch-Dest': 'empty',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Site': 'same-origin',
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"macOS"',
  'Pragma': 'no-cache'
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function shuffle(arr) {
  return arr.map(value => ({ value, sort: Math.random() }))
    .sort((a, b) => a.sort - b.sort)
    .map(({ value }) => value)
}

async function getList({ letters }) {
  let officerList = []

  for await (let letter of letters) {
    const response = await fetch(
      'https://nypdonline.org/api/reports/b805fa11-d5d2-43f7-8c23-1649f5d387f1/data', {
        method: 'POST',
        body: `[{"key":"@SearchName","values":[""]},{"key":"@LastNameFirstLetter","values":["${letter}"]}]`,
        headers
      }
    )
    const result = await response.json()
    let list = parseList(result)
    if (!list) {
      lettersRetry.set(letter, true)
      return
    }

    officerList.push(...list.officers)
  }

  return officerList
}

function parseList(data) {

  if (!data || !Array.isArray(data)) {
    console.error('error parsing list', data)
    return
  }

  const officers = data.map(entry => {
    const columns = entry.columns
    const taxid = parseInt(entry.filterRowValue.trim(), 10)

    let officer = {
      full_name: columns[0].value.trim(),
      rank: columns[1].value.trim(),
      shield_no: columns[2].value.trim(),
      appt_date: columns[3].value.split(' ')[0].trim(),
      recognition_count: parseInt(columns[5].value.trim(), 10),
      arrest_count: parseInt(columns[4].value.trim(), 10),
    }

    const last_name = officer.full_name.split(',')[0].trim()
    const first_part = officer.full_name.split(',')[1].trim()
    const first_name = first_part.split(' ')[0].trim()
    let middle_initial = ''
    if (first_part?.split(' ')[1]?.trim()) {
      middle_initial = first_part.split(' ')[1].trim()
    }
    officer = {
      taxid,
      full_name: officer.full_name,
      first_name,
      last_name,
      middle_initial,
      ...officer,
    }

    return officer
  })

  return {
    officers,
    total: officers.length
  }
}

async function getOfficer({ officer }) {
  officer.reports = {}

  const options = {
    method: 'POST',
    headers,
    body: `[{"key":"@TAXID","values":["${officer.taxid}"]}]`
  }

  let allReports = []
  try {
    allReports = await Promise.all([
      scheduleFetch({ url: reportList.summary, options }),
      scheduleFetch({ url: reportList.ranks, options }),
      scheduleFetch({ url: reportList.documents, options }),
      scheduleFetch({ url: reportList.discipline, options }),
      scheduleFetch({ url: reportList.arrests, options }),
      scheduleFetch({ url: reportList.training, options }),
      scheduleFetch({ url: reportList.awards, options })
    ])
  } catch(e) {
    console.error(`fetching reports failed ${officer.full_name} ${officer.taxid}`)
    officersRetry.set(officer.taxid, officer)
  }

  try {
    officer.reports.summary = parseSummary(allReports[0])

    // command was removed from list,
    // but copy it from summary for backward compatabilty
    if (!officer.command && officer.reports.summary?.command) {
      officer.command = officer.reports.summary.command
    }
    if (!officer.reports.summary) {
      officersRetry.set(officer.taxid, officer)
      console.error(`${officer.full_name} ${officer.taxid} missing summary`)
    }

    officer.reports.ranks = parseRanks(allReports[1])

    // should not be empty
    if (!officer.reports.ranks || officer.reports.ranks.length == 0) {
      officersRetry.set(officer.taxid, officer)
    }

    officer.reports.documents = parseDocuments(allReports[2])

    officer.reports.discipline = await getDiscipline({
      options,
      taxid: officer.taxid,
      discipline: parseDiscipline(allReports[3]),
      officer
    })

    officer.reports.arrests = parseArrests(allReports[4])

    officer.reports.training = parseTraining(allReports[5])

    // should not be empty and officer often has previous data in a previous run
    // retry doesnt seem to fix it
    //
    if (!officer.reports.training || officer.reports.training.length == 0) {
      console.error(`empty training ${officer.full_name} ${officer.taxid}`)
      officersRetry.set(officer.taxid, officer)
    }

    officer.reports.awards = parseAwards(allReports[6])
  } catch(e) {
    officersRetry.set(officer.taxid, officer)
    console.error(`parsing reports failed ${officer.full_name} ${officer.taxid}`, e)
  }

  // console.info(`${officer.full_name} ${officer.taxid}`)
  return officer
}

async function getDiscipline({ options, taxid, discipline, officer }) {
  if (!discipline) {
    console.error(`no discipline for charges ${officer.full_name} ${officer.taxid}`)
    return []
  }
  let disciplineEntries = []
  let chargesGroupId = 1
  let allegationsGroupId = 1
  for await (let entry of discipline) {
    let result

    try {
      if (entry.charges_count) {
        result = await scheduleFetch({ url: reportList.charges, options })
        entry.charges = parseDisciplineCharges(result, chargesGroupId++)
      }
      if (entry.allegations_count) {
        result = await scheduleFetch({ url: reportList.allegations, options: { ...options, body: `[{"key":"@TAXID","values":["ALLEG${taxid}"]}]` } })
        entry.allegations = parseDisciplineAllegations(result, allegationsGroupId++)
      }

      if (entry.charges_count !== entry.charges?.length) {
        console.info(`mismatch charges count ${officer.full_name} ${officer.taxid}`, entry)
        officersRetry.set(taxid, officer)
      }
      if (entry.allegations_count !== entry.allegations?.length) {
        console.info(`mismatch allegations count ${officer.full_name} ${officer.taxid}`, entry)
        officersRetry.set(taxid, officer)
      }

      disciplineEntries.push(entry)
    } catch(e) {
      console.error(`invalid discipline charges/allegations for ${officer.full_name} ${officer.taxid}`, e)
      officersRetry.set(taxid, officer)
    }
  }
  return disciplineEntries
}

function parseSummary(data) {
  return findValues({
    items: data?.[0]?.items,
    map: {
      command: 'Command:',
      assignment_date: 'Assignment Date:',
      ethnicity: 'Ethnicity:',
      rank_desc: 'Rank:',
      shield_no: 'Shield No:',
      appt_date: 'Appointment Date:'
    }
  })
}

function parseRanks(data) {
  if (!validData(data)) return
  let ranks = data.map(entry => {
    const items = entry.columns
    return {
      rank: items[1].value.trim(),
      date: items[0].value.split(' ')?.[0].trim(),
      shield_no: items[2].value.trim(),
    }
  })
  ranks.sort(sortRanks)
  return ranks
}

function correctDocUrl(url) {
  if (url.startsWith('https://oip-admin.nypdonline.local/')) {
    return url.replace('https://oip-admin.nypdonline.local/', 'https://oip.nypdonline.org/')
  } else {
    return 'https://oip.nypdonline.org' + url
  }
}

function parseDocuments(data) {
  if (!validData(data)) return
  let documents = data.map(document => {
    const items = document.columns
    let entry = {
      date: items[2].value.split(' ')?.[0].trim(),
      url: items[3].value.trim(),
      type: items[4].value.trim(),
    }
    entry.url = correctDocUrl(entry.url.split('"')[1])
    return entry
  })
  documents.sort(sortDocuments)
  return documents
}

function parseDiscipline(data) {
  if (!validData(data)) return
  return data.map(entry => {
    const items = entry.columns
    let e = {
      entry: items[0].value.trim(),
      charges_count: parseInt(items[1].value.trim(), 10),
      allegations_count: parseInt(items[2].value.trim(), 10),
    }
    if (e.charges_count == 0) delete e.charges_count
    if (e.allegations_count == 0) delete e.allegations_count
    return e
  })
}

function parseDisciplineCharges(data, groupId) {
  if (!validData(data)) return

  let charges = data.filter(charge => charge.groupId === '' + groupId).map(charge => {
    if (!charge.groupName.match('Penalty:')) {
      console.error('no penalty in penalty')
    }
    const group = charge.groupName.split('Penalty:')
    let penalty = cleanPenalty(group?.[1])

    const items = charge.columns
    let entry = {
      disposition: items[1].value.trim(),
      description: items[0].value.trim(),
    }
    if (penalty) {
      entry.penalty = penalty
    }
    return entry
  })

  return charges
}

function parseDisciplineAllegations(data, groupId) {
  if (!validData(data)) return

  let allegations = data.filter(allegation => allegation.groupId === '' + groupId).map(allegation => {
    const group = allegation.groupName.split('Penalty:')
    let penalty = cleanPenalty(group?.[1])

    const items = allegation.columns
    let entry = {
      recommendation: items[1].value.trim(),
      description: items[0].value.trim(),
    }
    if (entry.recommendation) { entry.recommendation = entry.recommendation.replace(/,$/, '') }
    if (penalty) {
      entry.penalty = penalty
    }
    return entry
  })

  return allegations
}

function parseArrests(data) {
  if (!validData(data)) return
  let arrests = {}
  data.forEach(arrest => {
    const items = arrest.columns
    let entry = {
      classification: items[0].value.trim(),
      arrest_count: items[1].value.trim(),
    }
    arrests[entry.classification.toLowerCase()] = parseInt(entry.arrest_count, 10)
  })
  return arrests
}

function parseTraining(data) {
  if (!validData(data)) return
  let trainings = data.map(entry => {
    const items = entry.columns
    return {
      date: items[0].value?.split(' ')?.[0].trim(),
      name: items[1].value.trim(),
    }
  })
  trainings.sort(sortByDateName)
  return trainings
}

function parseAwards(data) {
  if (!validData(data)) return
  let awards = data.map(entry => {
    const items = entry.columns
    return {
      date: items[0].value?.split(' ')[0].trim(),
      name: items[1].value.trim(),
    }
  })
  awards.sort(sortByDateName)
  return awards
}

function validData(data) {
  if (!data) return false
  if (!Array.isArray(data)) return false
  return true
}

function cleanPenalty(penalty) {
  if (penalty) {
    penalty = penalty.trim()
    penalty = penalty.replace(/&nbsp;/g, ' ')
    penalty = penalty.replace(/<i>/g, '')
    penalty = penalty.replace(/<\/div>/g, '')
    penalty = penalty.replace(/<\/i>/g, '')
  }

  return penalty
}

function findValues({ items, map }) {
  if (!items) {
    console.error('no items to find values in')
    return
  }

  let values = {}
  Object.keys(map).forEach(key => {
    let value = items.find(item => item.label === map[key]).value
    if (value) {
      value = value.trim()
      if (key.endsWith('_count')) {
        value = parseInt(value, 10)
      }
      // remove time portion of dates
      if (key.endsWith('_date')) {
        value = value.split(' ')[0]?.trim()
      }
    }
    values[key] = value
  })
  return values
}

function sortByDateName(a, b) {
  if (a.date === b.date) {
    if (a.name < b.name) return -1
    if (a.name > b.name) return 1
    return 0
  }
  if (!b.date) return -1
  if (!a.date) return 1
  return new Date(b.date) - new Date(a.date)
}

function sortDocuments(a, b) {
  if (a.date === b.date) {
    const byOfficers = compareOfficersList(a.officers, b.officers)
    if (byOfficers !== 0) return byOfficers
    if (a.url < b.url) return -1
    if (a.url > b.url) return 1
    return 0
  }
  if (!b.date) return -1
  if (!a.date) return 1
  return new Date(b.date) - new Date(a.date)
}

function compareOfficersList(a, b) {
  if (a === undefined && b === undefined) return 0
  if (a === undefined) return -1
  if (b === undefined) return 1
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    const compare = sortByOfficerName(a[i], b[i])
    if (compare !== 0) return compare
  }
  return a.length - b.length
}

function sortRanks(a, b) {
  if (a.date === b.date) {
    if (a.rank === b.rank) {
      if (a.shield_no < b.shield_no) return -1
      if (a.shield_no > b.shield_no) return 1
      return 0
    }
    if (a.rank < b.rank) return -1
    if (a.rank > b.rank) return 1
  }
  if (!b.date) return 1
  if (!a.date) return -1
  return new Date(a.date) - new Date(b.date)
}

function sortByOfficerName(a, b) {
  if (a.last_name < b.last_name) return -1
  if (a.last_name > b.last_name) return 1
  if (a.first_name < b.first_name) return -1
  if (a.first_name > b.first_name) return 1
  if (a.taxid > b.taxid) return 1
  if (a.taxid < b.taxid) return -1
  return 0
}

function scheduleFetch({ url, options }) {
  return scheduler.enqueue(() => fetch(url, options).then(response => {
    return response.json()
  }))
}

async function saveProfiles({ letter, officers }) {
  officers.sort(sortByOfficerName)

  let training = officers.map(officer => {
    const entry = {
      taxid: officer.taxid,
      full_name: officer.full_name,
      training: officer.reports?.training || []
    }
    return entry
  })

  const file = `nypd-profiles-${letter}.json`
  const data = JSON.stringify(officers.map(o => {
    delete o.reports?.training
    return o
  }), null, '\t')
  await fs.writeFile(file, data)

  let index = 1
  while (training.length) {
    const slice = training.splice(0,1000)
    fs.writeFile(`nypd-profiles-${letter}-training-${index}.json`, JSON.stringify(slice, null, '\t'))
    index++
  }
}

// Scrape trial decision docs from https://nypdonline.org/link/1016
// Most of these should already be included in the profile data, but in the case a trial decision gets posted
// when an officer no longer works for the department (e.g. they got fired) it'll show up here but not in the
// profile data (because their profile goes away when they leave).
async function scrapeTrialDecisions() {
  const result = await scheduleFetch({
    url: reportList.trialDecisions,
    options: { method: 'POST', body: '[]', headers },
  })

  if (!result?.length) {
    console.error('error fetching trial decisions')
    return
  }

  let data = []
  for (const row of result) {
    const items = row.columns
    let doc = {
      names: items[3].value.trim(),
      date: items[2].value.split(' ')[0].trim(),
      url: items[4].value
    }
    doc.url = correctDocUrl(doc.url.match(/<a href="([^"]+)"/)[1])

    let taxids = []
    if (items[3].filterValue) {
      taxids = items[3].filterValue.split(',')
    }
    let taxidIndex = 0
    doc.officers = doc.names.split('; ').map(name => {
      const [last_name, first_name] = name.replace(/<u>/g, '').replace(/<\/u>/g, '').split(', ')
      let officer = {
        last_name,
        first_name,
      }
      // Tax IDs are in the same order as the names, but skipped for officers
      // who are no longer active.
      if (first_name.endsWith('*')) {
        officer.retired = true
        officer.first_name = officer.first_name.slice(0, -1)
      } else {
        officer.taxid = taxids[taxidIndex++]
      }
      return officer
    })
    doc.officers.sort(sortByOfficerName)

    delete doc.names
    data.push(doc)
  }
  data.sort(sortDocuments)

  await fs.writeFile('trial-decisions.json', JSON.stringify(data, null, '\t'))
}

async function start() {
  const letters = shuffle('ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split(''))

  async function handleOfficers(officers) {
    let promises = []
    officers.forEach(officer => {
      promises.push(getOfficer({ officer }))
    })
    officers = await Promise.all(promises)
    return officers
  }

  async function handleLetters(letters) {
    for await (let letter of letters) {
      officersRetry = new Map()

      let officers = await getList({ letters: [letter] })
      officers = shuffle(officers)
      console.info(`fetching officer details ${letter} (${officers.length})`)
      officers = await handleOfficers(officers)

      if (officersRetry.size) {
        console.info(`retrying officers with errors`, Array.from(officersRetry.keys()))
        let retriedOfficers = await handleOfficers(Array.from(officersRetry.values()))
        retriedOfficers.forEach(officer => {
          const index = officers.findIndex(o => o.taxid === officer.taxid)
          if (index > -1) { officers[index] = officer }
        })
      }

      await saveProfiles({ letter, officers })
    }
  }

  await handleLetters(letters)
  if (lettersRetry.size) {
    console.info(`retrying letters with errors`, Array.from(lettersRetry.keys()))
    await handleLetters(Array.from(lettersRetry.keys()))
  }

  await scrapeTrialDecisions()
}

start()
