// read per-letter profile json, convert to joined csvs

import { promises as fs } from 'fs'
import * as d3 from 'd3';

async function loadFile({ letter, reports }) {
  let json = await fs.readFile(`nypd-profiles-${letter}.json`)
  let profiles = JSON.parse(json)

  if (reports.includes('training')) {
    let files = await fs.readdir('.')
    files = files.filter(f => f.startsWith(`nypd-profiles-${letter}-training-`))
    for (const file of files) {
      json = await fs.readFile(file)
      const training = JSON.parse(json)
      profiles = profiles.map(profile => {
        const entry = training.find(t => (t.taxid == profile.taxid))
        if (entry && entry.training.length) {
          const reports = profile.reports || {}
          reports.training = entry.training
          profile.reports = reports
        }
        return profile
      })
    }
  }

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
            group_id: charge.group_id,
            disposition: charge.disposition,
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
            group_id: allegation.group_id,
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
      const profiles = await loadFile({ letter, reports })

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
