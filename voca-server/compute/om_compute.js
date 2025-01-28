const fs = require("fs");


async function computeTableFlag(data,m3TableName) {
    const maxT = parseInt(data.rows[data.rows.length - 1]['t']);

    const bufLen = 2 ** Math.ceil(Math.log2(maxT));
    const tempArray = new Array(bufLen);
    data.rows.forEach(item => {
        tempArray[item['t']] = item['v'];
    });
    const arrayBuffer = new Buffer.alloc(bufLen);

    for (let j = 0; j < tempArray.length; j += 2) {
        if (tempArray[j] === undefined && tempArray[j + 1] === undefined) {
            continue;
        } else if (tempArray[j] === undefined) {
            arrayBuffer[j] = 1;
            arrayBuffer[j + 1] = 0;
            continue;
        } else if (tempArray[j + 1] === undefined) {
            arrayBuffer[j] = 1;
            arrayBuffer[j + 1] = 1;
            continue
        }
        if (tempArray[j] < tempArray[j + 1]) {
            arrayBuffer[j] = 0;
            arrayBuffer[j + 1] = 0;
        } else {
            arrayBuffer[j] = 0;
            arrayBuffer[j + 1] = 1;
        }
    }
    fs.writeFileSync(`./flags/${m3TableName}.flagz`, arrayBuffer);
    console.log("compute ordering flag finished:",m3TableName)
    //pool.end()
}

async function nonuniformMinMaxEncode(pool,rawTableName,om3TableName,mode) {
    let name=om3TableName;
    if(name.includes(".")){
        name=name.split(".")[1];
    }
    let fileName='';
    if(mode==='Custom'){
        fileName="custom_"+name;
    }
    const querySQL = `SELECT t,v FROM ${rawTableName}  ORDER by t ASC`
    const queryData = await pool.query(querySQL);
    computeTableFlag(queryData,fileName,mode);
    // return
    let data = queryData.rows;
    // console.log("DATA:");
    console.log("------------------")
    let min = parseFloat(data[0]['v']);
    let max = parseFloat(data[0]['v']);
    let maxTime = parseInt(data[0]['t']);  //maxTime's problem(Maybe)
    // console.log(min,max,maxTime);

    let sum = 0;
    for (let i = 0; i < data.length; i++) {
        sum = sum + parseFloat(data[i]['v']);
        if (parseFloat(data[i]['v']) < min) {
            min = data[i]['v'];
        }
        if (parseFloat(data[i]['v']) > max) {
            max = data[i]['v'];
        }
        if (parseInt(data[i]['t']) > maxTime) {
            maxTime = data[i]['t'];
        }
    }
    let ave = sum / data.length;
    console.log(min,max,maxTime,ave);
    const realLen = 2 ** Math.ceil(Math.log2(maxTime));
    let maxL = Math.ceil(Math.log2(maxTime));
    console.log("maxTime:"+maxTime,"maxL:"+maxL);
    // maxL = 23
    const dataArray = new Array(realLen);
    const lastItem=data[data.length-1];
    data.forEach((v, i) => {
        dataArray[v['t']] = v['v'];
    });
    data=null
    let curL = 1;
    let minV = dataArray
    let maxV = dataArray
    let aveV = dataArray
    for (let l = curL; l <= maxL; l++) {

        console.log("compute level:", l)

        let curMinVDiff = new Array(2 ** (maxL - l));
        let curMaxVDiff = new Array(2 ** (maxL - l));
        let curAveVDiff = new Array(2 ** (maxL - l));

        let curMinV = new Array(2 ** (maxL - l));
        let curMaxV = new Array(2 ** (maxL - l));
        let curAveV = new Array(2 ** (maxL - l));

        for (let i = 0; i < 2 ** (maxL - l + 1); i += 2) {

            //Min
            if (minV[i] === undefined && minV[i + 1] !== undefined) {
                curV = minV[i + 1]
                curDif = undefined;
            } else if (minV[i] !== undefined && minV[i + 1] === undefined) {
                curV = minV[i];
                curDif = 0;
            } else if (minV[i] === undefined && minV[i + 1] === undefined) {
                curV = undefined;
                curDif = undefined;
            } else {
                curV = Math.min(minV[i], minV[i + 1]);
                curDif = minV[i] - minV[i + 1];
            }
            curMinV[i / 2] = curV;
            curMinVDiff[i / 2] = curDif;

            //Max
            if (maxV[i] === undefined && maxV[i + 1] !== undefined) {
                curV = maxV[i + 1];
                curDif = 0;
            } else if (maxV[i] !== undefined && maxV[i + 1] === undefined) {
                curV = maxV[i];
                curDif = undefined;
            } else if (maxV[i] === undefined && maxV[i + 1] === undefined) {
                curV = undefined;
                curDif = undefined;
            } else {
                curV = Math.max(maxV[i], maxV[i + 1]);
                curDif = maxV[i] - maxV[i + 1];
            }
            curMaxV[i / 2] = curV;
            curMaxVDiff[i / 2] = curDif;

            //Ave
            let curAve;
            let curAveDif;
            if (parseFloat(aveV[i]) === undefined && parseFloat(aveV[i + 1]) !== undefined) {
                curAve = parseFloat(aveV[i + 1]) / 2;
                curAveDif = 0;
            } else if (parseFloat(aveV[i]) !== undefined && parseFloat(aveV[i + 1]) === undefined) {
                curAve = parseFloat(aveV[i]) / 2;
                curAveDif = undefined;
            } else if (parseFloat(aveV[i]) === undefined && parseFloat(aveV[i + 1]) === undefined) {
                curAve = undefined;
                curAveDif = undefined;
            } else {
                curAve = (parseFloat(aveV[i]) + parseFloat(aveV[i+1])) / 2;
                curAveDif = parseFloat(aveV[i]) - parseFloat(aveV[i + 1]);
            }
            curAveV[i / 2] = curAve;
            curAveVDiff[i / 2] = curAveDif;
        }
        minV = curMinV;
        maxV = curMaxV;
        aveV = curAveV;

        if (l === 1) {
            continue
            // console.log(curMinT, curMinV, curMaxV, curMaxT);
        }

        // console.log(aveV)
        let sqlStr = `insert into ${om3TableName}(i,minvd,maxvd,avevd) values `
        let i = 0;
        while (i < curMaxVDiff.length) {
            const usedL = maxL - l
            let tempStr = ''
            if (i + 10000 < curMaxVDiff.length) {
                for (let j = i; j < i + 10000; j++) {
                    if (curMinVDiff[j] === undefined && curMaxVDiff[j] === undefined) {
                        continue;
                    }

                    if (tempStr === '') {
                        tempStr += ` (${(2 ** usedL) + j},${curMinVDiff[j] === undefined ? "NULL" : curMinVDiff[j]},${curMaxVDiff[j] === undefined ? "NULL" : curMaxVDiff[j]},${curAveVDiff[j] === undefined ? "NULL" : curAveVDiff[j]})`;
                        // tempStr += ` (${(2 ** usedL) + j},${curMinVDiff[j] === undefined ? "NULL" : curMinVDiff[j]},${curMaxVDiff[j] === undefined ? "NULL" : curMaxVDiff[j]})`;
                    } else {
                        tempStr += `,(${(2 ** usedL) + j},${curMinVDiff[j] === undefined ? "NULL" : curMinVDiff[j]},${curMaxVDiff[j] === undefined ? "NULL" : curMaxVDiff[j]},${curAveVDiff[j] === undefined ? "NULL" : curAveVDiff[j]})`;
                    }
                }

            } else {
                for (let j = i; j < curMaxVDiff.length; j++) {
                    if (curMinVDiff[j] === undefined && curMaxVDiff[j] === undefined) {
                        continue;
                    }

                    if (tempStr === '') {
                        tempStr += ` (${(2 ** usedL) + j},${curMinVDiff[j] === undefined ? "NULL" : curMinVDiff[j]},${curMaxVDiff[j] === undefined ? "NULL" : curMaxVDiff[j]},${curAveVDiff[j] === undefined ? "NULL" : curAveVDiff[j]})`;
                    } else {
                        tempStr += `,(${(2 ** usedL) + j},${curMinVDiff[j] === undefined ? "NULL" : curMinVDiff[j]},${curMaxVDiff[j] === undefined ? "NULL" : curMaxVDiff[j]},${curAveVDiff[j] === undefined ? "NULL" : curAveVDiff[j]})`;
                    }
                }
            }
            i += 10000
            if (tempStr === '') {
                continue
            }
            let sql = sqlStr + tempStr;
            try {
                await pool.query(sql)
            } catch (err) {
                console.log(sql);
                console.log(err);
               // pool.end();
                throw err
            }
        }
    }
    if (min !== undefined && max !== undefined) {
        const l0Sql = `insert into ${om3TableName}(i,minvd,maxvd,avevd) values(${-1},${min},${max},${ave})`
        await pool.query(l0Sql);
        //pool.end()
    }
    return {
        maxLevel:maxL,
        maxLen:lastItem['t'],
        name:name,
    }
}

module.exports={nonuniformMinMaxEncode}


