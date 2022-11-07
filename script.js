// gather NYPD Officer Profile data
// https://nypdonline.org/link/2

import { promises as fs } from 'fs'
import fetch from 'node-fetch'
import { Scheduler } from 'async-scheduler'

const scheduler = new Scheduler(20)

const reportList = {
  summary:        'https://oip.nypdonline.org/api/reports/1/datasource/list',
  ranks:          'https://oip.nypdonline.org/api/reports/7/datasource/list',
  documents:      'https://oip.nypdonline.org/api/reports/2041/datasource/list',
  discipline:     'https://oip.nypdonline.org/api/reports/1031/datasource/list',
  charges:        'https://oip.nypdonline.org/api/reports/1030/datasource/list',
  allegations:    'https://oip.nypdonline.org/api/reports/2043/datasource/list',
  arrests:        'https://oip.nypdonline.org/api/reports/2042/datasource/list',
  awards:         'https://oip.nypdonline.org/api/reports/13/datasource/list',
  training:       'https://oip.nypdonline.org/api/reports/1027/datasource/list',
  trialDecisions: 'https://oip.nypdonline.org/api/reports/2038/datasource/list',
}

let lettersRetry = new Map()
let officersRetry = new Map()

let headers = {
  'Accept': 'application/json, text/plain, */*',
  'Content-Type': 'application/json;charset=UTF-8',
  'Pragma': 'no-cache'
}

async function getCookie() {
  const response = await fetch('https://oip.nypdonline.org/oauth2/token', {
    'body': 'grant_type=client_credentials&scope=clientId%3D435e66dd-eca9-47fc-be6b-091858a1ca7d',
    'method': 'POST'
  })
  const result = await response.json()
  return `user=${result.access_token}`
}

async function getList({ letters }) {
  let officerList = []

  for await (let letter of letters) {
    let page = 1
    let letterTotal
    let promises = []

    // fetch the first page to determine the number of pages to fetch
    const response = await fetch(
      getListQuery({ letter, page: 1 }),
      { method: 'GET', headers }
    )
    const result = await response.json()
    let list = parseList(result)
    if (!list) {
      lettersRetry.set(letter, true)
      return
    }

    letterTotal = list.total
    officerList.push(...list.officers)

    while (!letterTotal || ((page * 100) < letterTotal)) {
      page++
      promises.push(scheduleFetch({
        url: getListQuery({ letter, page }),
        options: { method: 'GET', headers }
      }))
    }

    try {
      const allResults = await Promise.all(promises)
      allResults.forEach(result => {
        let list = parseList(result)
        if (!list) {
          lettersRetry.set(letter, true)
          return
        }
        officerList.push(...list.officers)
      })
    } catch(e) {
      console.error('error fetching list')
      lettersRetry.set(letter, true)
    }
  }

  return officerList
}

function parseList(data) {

  if (!data || !data.Data) {
    console.error('error parsing list', data)
    return
  }

  const total = data.Total

  const officers = data.Data.map(entry => {
    const columns = entry.Columns
    const taxid = parseInt(entry.RowValue.trim(), 10)
    let officer = {}

    const map = [
      { field: 'full_name',         id: '85ed4926-7d4c-4771-a921-f5fe84ac2acc' },
      { field: 'command',           id: '634ce95e-3d6d-48f6-a4d2-08feb790da5c' },
      { field: 'rank',              id: '68ffdffb-f776-46cf-aac1-2b44d81d8ba4' },
      { field: 'shield_no',         id: '0c100529-fe3b-4bf0-8525-559b1a64f9b0' },
      { field: 'appt_date',         id: '8248c3a6-cefe-456d-be0a-92db9dc2e2d4' },
      { field: 'recognition_count', id: 'dedfe766-8ca6-4d0d-adc8-1aef44e6dbd8' },
      { field: 'arrest_count',      id: 'c10b2ff4-08a6-45f6-ab18-c6d06e6f43b2' }
    ]
    map.forEach(entry => {
      let value = columns.find(cell => cell.Id === entry.id).Value
      if (value) {
        value = value.trim()
        if (entry.field.endsWith('_count')) {
          value = parseInt(value, 10)
        }
        if (entry.field.endsWith('_date')) {
          value = value.split(' ')[0].trim()
        }
      }
      officer[entry.field] = value
    })

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
    total
  }
}

async function getOfficer({ officer }) {
  officer.reports = {}

  const options = {
    method: 'POST',
    headers,
    body: `{"filters":[{"key":"@TAXID","label":"TAXID","values":["${officer.taxid}"]}]}`
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
    console.error(`fetching reports failed ${officer.full_name}`)
    officersRetry.set(officer.taxid, officer)
  }

  try {
    officer.reports.summary = parseSummary(allReports[0])

    if (!officer.reports.summary) {
      officersRetry.set(officer.taxid, officer)
      console.error(`${officer.full_name} missing summary`)
    }

    officer.reports.ranks = parseRanks(allReports[1])
    // should not be empty but persists
    //
    // if (!officer.reports.ranks || officer.reports.ranks.length == 0) {
    //   console.error(`empty ranks ${officer.full_name}`)
    //   officersRetry.set(officer.taxid, officer)
    // }

    officer.reports.documents = parseDocuments(allReports[2])

    officer.reports.discipline = await getDiscipline({
      options,
      taxid: officer.taxid,
      discipline: parseDiscipline(allReports[3]),
      officer
    })

    officer.reports.arrests = parseArrests(allReports[4])

    officer.reports.training = parseTraining(allReports[5])
    // should not be empty but persists
    //
    // if (!officer.reports.training || officer.reports.training.length == 0) {
    //   console.error(`empty training ${officer.full_name}`)
    //   officersRetry.set(officer.taxid, officer)
    // }

    officer.reports.awards = parseAwards(allReports[6])
  } catch(e) {
    officersRetry.set(officer.taxid, officer)
    console.error(`parsing reports failed ${officer.full_name}`)
  }

  return officer
}

async function getDiscipline({ options, taxid, discipline, officer }) {
  if (!discipline) {
    console.error('no discipline for charges')
    return []
  }
  let disciplineEntries = []
  for await (let entry of discipline) {
    const body = `{"filters":[{"key":"@TAXID","label":"TAXID","values":["${taxid}"]},{"key":"@DATE","values":["${entry.entry}"]}]}}`
    let result

    try {
      if (entry.charges_count) {
        result = await scheduleFetch({ url: reportList.charges, options: { ...options, body } })
        entry.charges = parseDisciplineCharge(result, entry)
      }
      if (entry.allegations_count) {
        result = await scheduleFetch({ url: reportList.allegations, options: { ...options, body } })
        entry.allegations = parseDisciplineAllegation(result, entry)
      }
      disciplineEntries.push(entry)
    } catch(e) {
      console.error('invalid discipline charges/allegations')
      officersRetry.set(taxid, officer)
    }
  }
  return disciplineEntries
}

function parseSummary(data) {
  return findValues({
    items: data?.[0]?.Items,
    map: {
      command: '1692f3bf-ed70-4b4a-96a1-9131427e4de9',
      assignment_date: '8a2bcb6f-e064-44f4-8a58-8f38aa6ebae9',
      ethnicity: '0ec90f94-b636-474c-bec7-ab04e73540ed',
      rank_desc: 'a2fded09-5439-4b17-9da8-81a5643ec3e8',
      shield_no: '42f74dfc-ee54-4b25-822f-415615d22aa9',
      appt_date: '20e891ce-1dcf-4d46-9185-075336788d65'
    }
  })
}

function parseRanks(data) {
  if (!validData(data)) return
  let ranks = data.map(entry => {
    return findValues({
      items: entry.Columns,
      map: {
        rank:      '31d512d9-6bac-45d4-8ab2-cbd951e3f216',
        date:      '74cead80-e1af-4aa3-9fa0-1dbf30bdf55b',
        shield_no: 'a5a69be2-3fe2-41d6-b174-b6c623cbe702',
      }
    })
  })
  ranks.sort(sortRanks)
  return ranks
}

function parseDocuments(data) {
  if (!validData(data)) return
  let documents = data.map(document => {
    let entry = findValues({
      items: document.Columns,
      map: {
        date: '0ecf6c5c-f9a1-4c90-9203-d6518e62937f',
        url:  'd6458572-9ecb-438c-8f8e-c56e070c91ba',
        type: '0ad88646-ece6-405a-bb43-ae0c4b7ba666',
        // year:  '7106df93-db85-4614-a35f-2a9c11be0432',
        // month: 'e64cb85b-c3eb-4087-b170-4ac1b30cde30',
        // day:   '81b47568-2508-4b0a-b106-72c1ffe521b1',
      }
    })
    entry.url = 'https://oip.nypdonline.org' + entry.url.split('"')[1]
    return entry
  })
  documents.sort(sortDocuments)
  return documents
}

function parseDiscipline(data) {
  if (!validData(data)) return
  return data.map(entry => {
    let e = findValues({
      items: entry.Columns,
      map: {
        entry:         '56baedfe-465d-4812-8dae-9bf94c240bbe', // is date, but also key
        charges_count: 'e495a851-c40e-4d96-9eb6-96352ce069df',
        allegations_count: '61dd971c-e4ba-42a6-8067-a1445bfee1e7'
      }
    })
    if (e.charges_count == 0) delete e.charges_count
    if (e.allegations_count == 0) delete e.allegations_count
    return e
  })
}

function parseDisciplineCharge(data) {
  if (!validData(data)) return
  return data.map(charge => {
    if (!charge.GroupName.match('Penalty:')) {
      console.error('no penalty in penalty')
    }
    const group = charge.GroupName.split('Penalty:')
    let penalty = group[1].trim()

    // cleanup formatting in penalty
    penalty = penalty.replace(/&nbsp;/g, ' ')
    penalty = penalty.replace(/<i>/g, '')
    penalty = penalty.replace(/<\/div>/g, '')
    penalty = penalty.replace(/<\/i>/g, '')

    let entry = findValues({
      items: charge.Columns,
      map: {
        disposition: '89d621a3-195c-4d07-b553-34d82a782012',
        command:     'a11835db-a8f3-4c40-8db5-71685a85f500',
        case_no:     '358a34a8-0e10-479b-b04f-a0838220cba8',
        description: 'ce5bb063-0f02-46ab-888a-b96c598e3c71'
      }
    })
    if (penalty) {
      entry.penalty = penalty
    }
    return entry
  })
}

function parseDisciplineAllegation(data) {
  let allegations = data.map(allegation => {

    const group = allegation.GroupName.split('Penalty:')
    let penalty = group?.[1]?.trim()
    if (penalty) {
      penalty = penalty.replace(/&nbsp;/g, ' ')
      penalty = penalty.replace(/<i>/g, '')
      penalty = penalty.replace(/<\/div>/g, '')
      penalty = penalty.replace(/<\/i>/g, '')
    }

    let entry = findValues({
      items: allegation.Columns,
      map: {
        recommendation: '56d6e87d-d665-4c20-aa2c-0238a11b1816',
        case_no:        '7946f55e-8d1d-4384-8c75-eca46968cccf',
        description:    'eace1634-fdc7-4fa1-ba60-58dc23643872'
      }
    })
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
    let entry = findValues({
      items: arrest.Columns,
      map: {
        classification: '984f6c06-6898-4c5d-8cc2-c7cf0dc7394e',
        arrest_count:   '26eb8cd3-e8cf-4494-a9f7-9a78de429d34'
      }
    })
    arrests[entry.classification.toLowerCase()] = parseInt(entry.arrest_count, 10)
  })
  return arrests
}

function parseTraining(data) {
  if (!validData(data)) return
  let trainings = data.map(entry => {
    return findValues({
      items: entry.Columns,
      map: {
        date: '51a518fa-b16b-421c-8c72-c713ecfc5583',
        name: '86c44195-8f19-4aac-bfb0-2bedc5a8047f'
      }
    })
  })
  trainings.sort(sortByDateName)
  return trainings
}

function parseAwards(data) {
  if (!validData(data)) return
  let awards = data.map(entry => {
    return findValues({
      items: entry.Columns,
      map: {
        date: 'ef49fd43-d1a3-4782-ab69-438c0ed05752',
        name: '6021827e-ebd8-473e-934e-867ebcbc8ce6'
      }
    })
  })
  awards.sort(sortByDateName)
  return awards
}

function validData(data) {
  if (!data) return false
  if (!Array.isArray(data)) return false
  return true
}

function findValues({ items, map }) {
  if (!items) {
    console.error('no items to find values in')
    return
  }

  let values = {}
  Object.keys(map).forEach(key => {
    let value = items.find(item => item.Id === map[key]).Value
    if (value) {
      value = value.trim()
      if (key.endsWith('_count')) {
        value = parseInt(value, 10)
      }
      // remove time portion of dates
      if (key.endsWith('date')) {
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
  return new Date(b.date) - new Date(a.date);
}

function sortDocuments(a, b) {
  if (a.date === b.date) {
    if (a.url < b.url) return -1
    if (a.url > b.url) return 1
    return 0
  }
  if (!b.date) return -1
  if (!a.date) return 1
  return new Date(b.date) - new Date(a.date);
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
  return new Date(a.date) - new Date(b.date);
}

function sortByOfficerName(a, b) {
  if (a.last_name < b.last_name) return -1
  if (a.last_name > b.last_name) return 1
  if (a.first_name < b.first_name) return -1
  if (a.first_name > b.first_name) return 1
  if (a.taxid > b.taxid) return 1
  if (a.taxid < b.taxid) return -1
}

function scheduleFetch({ url, options }) {
  return scheduler.enqueue(() => fetch(url, options).then(response => {
    return response.json()
  }))
}

function getListQuery({ letter, page }) {
  return `https://oip.nypdonline.org/api/reports/2/datasource/serverList?aggregate=&filter=&group=&page=${page}&pageSize=100&platformFilters=%7B%22filters%22:%5B%7B%22key%22:%22@SearchName%22,%22label%22:%22Search+Name%22,%22values%22:%5B%22SEARCH_FILTER_VALUE%22%5D%7D,%7B%22key%22:%22@LastNameFirstLetter%22,%22label%22:%22Last+Name+First+Letter%22,%22values%22:%5B%22${letter}%22%5D%7D%5D%7D&sort=`
}

async function saveProfiles({ letter, officers }) {
  officers.sort(sortByOfficerName)
  const file = `nypd-profiles-${letter}.json`
  const data = JSON.stringify(officers, null, '\t')
  await fs.writeFile(file, data)
}

// Scrape trial decision docs from https://oip.nypdonline.org/view/1006///%7B%22hideMobileMenu%22:true%7D/true/true
// Most of these should already be included in the profile data, but in the case a trial decision gets posted
// when an officer no longer works for the department (e.g. they got fired) it'll show up here but not in the
// profile data (because their profile goes away when they leave).
async function scrapeTrialDecisions() {
  headers.Cookie = await getCookie()

  const result = await scheduleFetch({
    url: reportList.trialDecisions,
    options: { method: 'GET', headers },
  })

  let data = []
  for (const row of result) {
    let doc = findValues({
      items: row.Columns,
      map: {
        names: 'aa970ef7-62f1-4d89-bcb8-5078017ee41a',
        date: '8a6d6957-716f-4f3c-998f-e4aebbdbe912',
        url: '0be5eacf-3870-4351-aa42-439067baadbe',
      },
    })
    doc.url = 'https://oip.nypdonline.org' + doc.url.match(/<a href="([^"]+)"/)[1]

    const nameCol = row.Columns.find(item => item.Id === 'aa970ef7-62f1-4d89-bcb8-5078017ee41a')
    let taxids = []
    if (nameCol.ColumnValue) {
      taxids = nameCol.ColumnValue.split(',')
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
  const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('')

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

      headers.Cookie = await getCookie()
      let officers = await getList({ letters: [letter] })
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
