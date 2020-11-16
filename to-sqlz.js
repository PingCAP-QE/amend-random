const fs = require('fs')
const path = require('path')

function main() {
    const dir = process.argv[2]
    if (!dir) {
        console.log('invalid given directory')
        return
    }

    const logs = fs.readdirSync(dir)
    const ordered = []
    for (const log of logs) {
        if (!log.endsWith('.log')) {
            continue
        }
        const s = log.split('.')
        s.pop()
        const thread = s.join('.').split('-').join('_')
        const content = fs.readFileSync(path.join(dir, log), { encoding: 'utf-8' })
        for (const line of content.split('\n')) {
            let match = /^\[[A-Z]+\] \[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{5})-(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{5})\] (.*)$/.exec(line)
            if (!match) continue
            let startTime = match[1]
            let sql = match[3]
            let [t1, t2] = startTime.split('.')
            ordered.push({
                thread,
                sql,
                t1: new Date(t1).getTime(),
                t2: parseInt(t2),
            })
        }
    }

    ordered.sort((a, b) => {
        if (a.t1 != b.t1) {
            return a.t1 - b.t1
        }
        return a.t2 - b.t2
    })

    let out = ''
    for (const log of ordered) {
        out += `/* ${log.thread} */ ${log.sql};\n`
    }

    fs.writeFileSync(path.join(dir, 'sqlz.sql'), out)
}

main()
