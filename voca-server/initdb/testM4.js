function computeM4TimeSE(width,timeRange){

    const res = []
    for(i = 0;i<width;i ++){
        res.push(new M4())
    }

    let globalStart = timeRange[0]
    let globalEnd = timeRange[1]

    //timeRangeLength个点，分给width个桶
    const timeRangeLength = globalEnd - globalStart + 1

    // 平均每个桶，分的点数
    const everyNum = timeRangeLength/width

    // 第一个M4，以globalStart开始
    res[0].start_time = globalStart;
    //res[0].end_time = Math.ceil(everyNum) - 1


    for(i = 1;i<width;i ++){

        // 当前M4开始，是上一个M4开始+平均每个桶分的点数，向上取整
        res[i].start_time=Math.ceil( i * everyNum)

        // 上一个M4结尾，是下一个M4开始-1
        res[i-1].end_time = res[i].start_time - 1

    }

    //最后一个M4，以globalEnd结尾
    res[width-1].end_time=globalEnd

    return res
}

 
 // 两表分别从数据库取出来，程序做加法，程序做M4
 async function Case1(table1, table2, width, symbol ,extremes){
    console.log('Case1')


    let sql = `SELECT ${table1}.t AS t, ${table1}.v AS v FROM ${table1} order by t asc `
    let result1 = await pool.query(sql);
    sql = `SELECT ${table2}.t AS t, ${table2}.v AS v FROM ${table2} order by t asc`
    let result2 = await pool.query(sql);


    // todo 两表相加，并输出width的M4数组
    let t3 = new Array(result2.rows.length)

    switch(symbol){
        case '+':
            for(let i=0;i<result1.rows.length;i++){
                let t=result1.rows[i].t
                let v = (result1.rows[i].v + result2.rows[i].v)

                //console.log(result1.rows[i].v , result2.rows[i].v,result1.rows[i].v + result2.rows[i].v)
                
                let pair = { t: t, v: v };
                t3[i] = pair
            };
            break;
        case '-':
            for(let i=0;i<result1.rows.length;i++){
                let t=result1.rows[i].t
                let v = (result1.rows[i].v - result2.rows[i].v)
                
                let pair = { t: t, v: v };
                t3[i] = pair
            };
            break;
        case '*':
            for(let i=0;i<result1.rows.length;i++){
                let t=result1.rows[i].t
                let v = (result1.rows[i].v * result2.rows[i].v)
                
                let pair = { t: t, v: v };
                t3[i] = pair
            }
            break;
        case '/':
            for(let i=0;i<result1.rows.length;i++){
                let t=result1.rows[i].t
                let v = (result1.rows[i].v / result2.rows[i].v)
                
                let pair = { t: t, v: v };
                t3[i] = pair
            }
            break;
    }


    let num = t3.length


    let PARTITION = Math.floor(num/width)

    let res = computeM4TimeSE(width, [0, num - 1])
    // let min_arr = []
    // let max_arr = []
    res.forEach(e =>{
        let min = Infinity;
        let max = -Infinity;
        for(let i = e.start_time; i <= e.end_time; i++){

            if(t3[i].v < min){
                min = t3[i].v
            }

            if(t3[i].v > max){
                max = t3[i].v
            }
        }
        e.min = min
        e.max = max
        e.st_v = t3[e.start_time].v
        e.et_v = t3[e.end_time].v
    })

    outputM4(res)

    return res;
}