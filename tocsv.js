// read per-letter profile json, convert to joined csvs

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
      ethnicity: profile.reports.summary?.ethnicity,
      assignment_date: profile.reports.summary?.assignment_date
    }
    const classifications = ['infraction', 'misdemeanor', 'felony', 'violation', 'other']
    classifications.forEach(classification => {
      const key = `arrests_${classification}`.toLowerCase()
      officer[key] = profile.reports.arrests?.[classification]
    })
    delete officer.reports
    return officer
  })
}

function getDiscipline({ profiles }) {
  let discipline = []
  profiles.forEach(profile => {
    profile.reports.discipline.forEach(entry => {
      if (entry.charges) {
        entry.charges.forEach(charge => {
          discipline.push({
            taxid: profile.taxid,
            date: entry.entry.split(' ')[0],
            disposition: charge.disposition,
            command: charge.command,
            case_no: charge.case_no,
            description: charge.description,
            penalty: charge.penalty,
            type: 'charge'
          })
        })
      }
      if (entry.allegations) {
        entry.allegations.forEach(allegation => {
          discipline.push({
            taxid: profile.taxid,
            date: entry.entry.split(' ')[0],
            case_no: allegation.case_no,
            description: allegation.description,
            recommendation: allegation.recommendation,
            penalty: allegation.penalty,
            type: 'allegation'
          })
        })
      }
    })
  })
  return discipline
}

function getReport({ profiles, report }) {
  let rows = []
  profiles.forEach(profile => {
    if (!profile?.reports?.[report]) { return }
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
  const letters = [...'ABCDEFGHIJKLMNOPQRSTUVWXYZ']

  let reports = ['officers', 'discipline', 'ranks', 'documents', 'awards', 'training']
  if (process.argv.slice(2).length) {
    reports = process.argv.slice(2)
  }

  for await (let report of reports) {
    let results = []

    for await (let letter of letters) {
      const profiles = await loadFile({ letter })

      if (report === 'officers') {
        results = results.concat(getOfficers({ profiles }))
      } else if (report === 'discipline') {
        results = results.concat(getDiscipline({ profiles }))
      } else {
        results = results.concat(getReport({ profiles, report }))
      }
    }

    // chunk writing csv to avoid node max str length
    let chunk = results.splice(0, 1000000)
    await fs.writeFile(`${report}.csv`, d3.csvFormat(chunk))
    while (results.length) {
      let chunk = results.splice(0, 1000000)
      await fs.appendFile(`${report}.csv`, d3.csvFormatBody(chunk))
    }
  }
}

start()
