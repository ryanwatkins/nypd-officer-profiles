// read per-letter profile json, covert to joined csvs

const fs = require('fs').promises
const d3 = require('d3')

async function loadFile({ letter }) {
  const json = await fs.readFile(`nypd-profiles-${letter}.json`)
  const profiles = JSON.parse(json)
  return profiles
}

function getOfficers({ profiles }) {
  return profiles.map(profile => {
    let officer = {
      ...profile,
      ethnicity: profile.reports.summary?.ethnicity
    }
    const classifications = ['infraction', 'misdemeanor', 'felony', 'violation', 'other']
    classifications.forEach(classification => {
      const key = `arrests_${classification}`.toLowerCase()
      officer[key] = profile.reports.arrests[classification]
    })
    delete officer.reports
    return officer
  })
}

function getDiscipline({ profiles }) {
  let discipline = []
  profiles.forEach(profile => {
    profile.reports.discipline.forEach(entry => {
      entry.charges.forEach(charge => {
        discipline.push({
          taxid: profile.taxid,
          date: entry.entry.split(' ')[0],
          ...charge
        })
      })
    })
  })
  return discipline
}

function getReport({ profiles, report }) {
  let rows = []
  profiles.forEach(profile => {
    profile.reports[report].forEach(entry => {
      rows.push({
        taxid: profile.taxid,
        ...entry
      })
    })
  })
  return rows
}

async function start() {
  const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('')
  const reports = ['ranks', 'documents', 'awards', 'training']

  let results = {
    officers: [],
    discipline: []
  }
  reports.forEach(report => { results[report] = [] })

  for await (let letter of letters) {
    const profiles = await loadFile({ letter })

    results.officers.push(...getOfficers({ profiles }))
    results.discipline.push(...getDiscipline({ profiles }))

    reports.forEach(report => {
      results[report] = results[report].concat(getReport({ profiles, report }))
    })
  }

  await fs.writeFile('officers.csv', d3.csvFormat(results.officers))
  await fs.writeFile('discipline.csv', d3.csvFormat(results.discipline))

  for await (let report of reports) {
    await fs.writeFile(`${report}.csv`, d3.csvFormat(results[report]))
  }
}

start()
