
const yargs = require("yargs")
const axios = require('axios')
const fs = require('fs');
const https = require('https');
const StreamZip = require('node-stream-zip');

var parseString = require('xml2js').parseString;
const options = yargs
    .usage("Cách dùng: -n <name>")
    .option("t", { alias: "timeframe", describe: "Timeframe ", type: "string", demandOption: false })
    .argv;

var timeframe = options.timeframe
timeframe = '1h'
function getAllSymbol() {
    let allList = [

        {
            link: 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/spot/monthly/klines/',
            type: 'spot'
        },
        {
            link: 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/monthly/klines/',
            type: 'future'
        }

    ]
    let listLink = []
    allList.map(link => {
        listLink.push(getListLink(link))
    })
    let configFile = []
    Promise.all(listLink).then(async data => {
        data = data.flat();
        for (let i = 0; i < data.length; i++) {
            console.log(`*${i + 1}/${data.length}.Begin download ${data[i].name} `)
            //  let result = await beginDownload(data[i].link, data[i].name, timeframe, data[i].type)
            //make configFile
            let linkGithub =`https://raw.githubusercontent.com/btm2021/binanceHistoryData/main/${(data[i].type=='spot')?'spot':'future'}/${timeframe}_${data[i].name}.json`
            configFile.push({
                name: data[i].name,
                timeframe,
                type: data[i].type,
                link:linkGithub
            })
        }
        fs.writeFileSync('config.json', JSON.stringify(configFile), {})
        console.log('writeDone')
    })
}
async function getListLink(link) {
    return new Promise((resolve, reject) => {
        fetchData(link.link).then(data => {
            var xml = data
            let listLink = []
            parseString(xml, (err, result) => {
                result.ListBucketResult.CommonPrefixes.map((i) => {
                    //   console.log(i)
                    listLink.push({
                        link: i.Prefix[0],
                        name: i.Prefix[0].split("klines")[1].replaceAll("/", ""),
                        type: link.type
                    })
                })
            })
            resolve(listLink)


        })
    })
}
getAllSymbol();
async function beginDownload(link, name, timeframe, type) {
    return new Promise((resolve, reject) => {
        let listLink = []
        let url = ''
        if (type === 'spot') {
            url = `https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/?prefix=data/spot/monthly/klines/${name}/${timeframe}/`
        } else {
            url = `https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/monthly/klines/${name}/${timeframe}/`
        }

        fetchData(url).then(data => {
            var xml = data
            parseString(xml, (err, result) => {
                result.ListBucketResult.Contents.map((i, index) => {
                    if (!i.Key[0].includes('CHECKSUM')) {
                        listLink.push({
                            url: 'https://data.binance.vision/' + i.Key[0],
                            type: type,
                            name: `${type}-${name}-${timeframe}-${index}.zip`,
                        })
                    }
                })

            });
            //  console.log(listLink)
            let downloadList = []
            listLink.map((item, index) => {
                downloadList.push(downloadFile(item.url, item.name))
            })
            Promise.all(downloadList).then(data => {
                //  console.log(data)
                //bat dau giai nen vao thu muc extracted

                var listZip = []
                var listFile = []
                data.map((item, index) => {
                    const zipFile = new StreamZip({ file: item });
                    listFile.push(item)
                    listZip.push(zipFile)
                })
                var allContent = []
                var allFile = []
                listZip.map((item, index) => {
                    allContent.push(readFileZip(item))

                })

                listFile.map(item => {
                    allFile.push(deleteFile(item))
                })

                Promise.all(allContent).then(data => {
                    mergeCSVFiles('extracted', name, type).then(data => {
                        resolve('ok')
                    })
                })

            })
        })
    })


}
async function deleteFile(filePath) {
    return new Promise((resolve, reject) => {
        fs.unlink('./' + filePath, (err) => {
            resolve('ok')
        });
    })

}
async function readFileZip(zip) {
    return new Promise((resolve, rject) => {
        zip.on('ready', () => {
            zip.extract(null, './extracted', (err, count) => {
                zip.close();
                resolve('ok')
            });
        });
    })
}
async function downloadFile(url, name) {
    return new Promise((resolve, reject) => {
        const file = fs.createWriteStream(name);
        const request = https.get(url, (response) => {
            response.pipe(file);
            // after download completed close filestream
            file.on("finish", () => {
                file.close();
                resolve(name)
            });
        });

    })


}
async function fetchData(url) {
    // make http call to url
    let response = await axios(url).catch((err) => console.log(err));

    if (response.status !== 200) {
        console.log("Error occurred while fetching data");
        return;
    }
    return response.data;
}
const papa = require("papaparse");
async function mergeCSVFiles(folderPath, name, type) {
    return new Promise((resolve, reject) => {
        let mergedData = [];

        fs.readdir(folderPath, (err, files) => {
            if (err)
                console.log(err);
            else {
                let listCSV = []
                files.forEach(file => {
                    listCSV.push(readFileCSV(`${folderPath}/${file}`))

                })
                Promise.all(listCSV).then(data => {
                    let mergedData = formatCSV(data)
                    fs.writeFile(`${type}/${timeframe}_${name}.json`, JSON.stringify(mergedData), 'utf-8', (error, data) => {
                        let listDelCSV = []
                        files.forEach(file => {
                            listDelCSV.push(deleteFileCSV(`${folderPath}/${file}`))
                        })
                        if (error) {
                            console.log(error)
                        }
                        Promise.all(listDelCSV).then(data => {
                            resolve('ok')
                        })
                    })
                })
            }
        })
    })


}
function formatCSV(data) {
    let fullList = []
    data.map(item => {
        let data1 = item.data
        data1.map(item1 => {
            fullList.push(item1)
        })
    })
    //parseFloat 
    let newList = []
    fullList.map(item => {
        const convertToFloat = item.map(str => parseFloat(str));
        newList.push(convertToFloat)
    })
    //sort
    newList.sort((a, b) => a[0] - b[0])
    return newList
}
async function writeFileCSV() {

}
async function deleteFileCSV(file) {
    return new Promise((resolve, rject) => {
        fs.unlink(file, (err) => {
            if (err) {
                console.log(err)
            }
            resolve('ok')
        })
    })
}
async function readFileCSV(file) {
    return new Promise((resolve, rject) => {
        let file2 = fs.createReadStream(file);
        papa.parse(file2, {
            complete: (results, file) => {
                resolve(results)
            }
        });
    })

}