
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

var name = options.name
var timeframe = options.timeframe
var type = options.type
//name = 'BANDUSDT'
timeframe = '1h'
// type = 'future'
//const greeting = `Hello, ${options.name}!`;
//beginDownload()
function getAllSymbol() {
    let allList = [
        'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/spot/monthly/klines/',
        'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/monthly/klines/'
    ]
    let listLink = []
    allList.map(link => {
        listLink.push(getListLink(link))
    })
    Promise.all(listLink).then(data => {
        let fullListLink = []
        console.log(data)
        
    })
}
async function getListLink(link) {
    return new Promise((resolve, reject) => {
        fetchData(link).then(data => {
            var xml = data
            let listLink = []
            parseString(xml, (err, result) => {
                result.ListBucketResult.CommonPrefixes.map((i) => {
                    //   console.log(i)
                    listLink.push({
                        link: i.Prefix[0],
                        name: i.Prefix[0].split("klines")[1].replaceAll("/", "")
                    })
                })
            })
            resolve(listLink)


        })
    })
}
getAllSymbol();
function beginDownload() {
    let listLink = []
    let url = `https://data.binance.vision/?prefix=data/${(type === 'future') ? 'futures' : 'spot'}/um/monthly/klines/${name}/${timeframe}/`
    console.log(url)

    url = `https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/monthly/klines/${name}/${timeframe}/`
    fetchData(url).then(data => {
        var xml = data
        parseString(xml, (err, result) => {
            result.ListBucketResult.Contents.map((i, index) => {
                if (!i.Key[0].includes('CHECKSUM')) {
                    //https://data.binance.vision/data/futures/um/monthly/klines/BANDUSDT/1h/BANDUSDT-1h-2023-06.zip
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
        console.log('* Begin download ')
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
                console.log('Unzip done.delete all zip file')
                mergeCSVFiles('extracted')
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
                console.log(` -- .${name} done.`);
                resolve(name)
            });
        });

    })


}
async function fetchData(url) {
    console.log("Crawling data...")
    // make http call to url
    let response = await axios(url).catch((err) => console.log(err));

    if (response.status !== 200) {
        console.log("Error occurred while fetching data");
        return;
    }
    return response.data;
}
const papa = require("papaparse");
function mergeCSVFiles(folderPath) {
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
                fs.writeFile(`${type}/${name}.json`, JSON.stringify(mergedData), 'utf-8', (error, data) => {
                    let listDelCSV = []
                    files.forEach(file => {
                        listDelCSV.push(deleteFileCSV(`${folderPath}/${file}`))
                    })
                    if (error) {
                        console.log(error)
                    }
                    Promise.all(listDelCSV).then(data => {
                        console.log('Done!')
                    })
                })
            })
        }
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