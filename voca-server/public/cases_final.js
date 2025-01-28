

const fs = require("fs");
//const { Pool } = require('pg');
const { Pool, types } = require('pg');
const { InfluxDB } = require('@influxdata/influxdb-client');
// 将NUMERIC类型的数据自动转换为浮点数
types.setTypeParser(1700, (val) => parseFloat(val));




// const { Heap }  = require('heap-js');
const { get } = require("http");

const dbConfig = {
    "hostname":"127.0.0.1",
    "db":"postgres",
    "username":"huangshuo",
    "password":"123456",
    "influxUrl":"http://localhost:8086",
    "token":"FYoCWAAlkz63FKy5nK-A8VrvHoMeRoyIEILmGL_VaSSuuj7XZrqXv7UEfS05xU3kMwZ7UXYULJIf_nhtN8m31w==",
    "org":"cxsj",
    "bucket":"cxsj"
}

let debug = false

const pool = new Pool({
    user: dbConfig['username'],
    host: dbConfig["hostname"],
    database: dbConfig['db'],
    password: dbConfig['password'],
});



let queryCounts = 0
let nodeCount = 0

let element = {
    value:0,
    nodePairs: null
};


let treeCache = null

// class MaxHeap{
//     constructor(){
//         const elementMaxComparator = (a, b) => b.value - a.value;
//         this.heap = new Heap(elementMaxComparator);
//     }

//     add(elements){
//         this.heap.push(elements);
//     }
    
//     isEmpty(){
//         return this.heap.length == 0;
//     }
    
//     pop(){
//         return this.heap.pop();
//     }
    
//     getTop(){
//         return this.heap.peek();
//     }
// }

// class MinHeap{
//     constructor(){
//         const elementComparator = (a, b) => a.value - b.value;
//         this.heap = new Heap(elementComparator);
//     }

//     add(elements){
//         this.heap.push(elements);
//     }
    
//     isEmpty(){
//         return this.heap.length == 0;
//     }
    
//     pop(){
//         return this.heap.pop();
//     }
    
//     getTop(){
//         return this.heap.peek();
//     }

   
// }

const MAXNODENUM = 1000 * 10000

// 定义 SegmentTreeNode 类
class SegmentTreeNode {
    constructor(sTime, eTime, level, index, i, min = 0, max = 0, id, 
        minDiff = null, maxDiff = null, leftChild = null, rightChild = null, leftIndex = null, rightIndex = null, parent = null, isBuild=false) {
        this.sTime = sTime;       // 开始时间
        this.eTime = eTime;       // 结束时间
        this.level = level;       // 层级
        this.index = index;       // 当前节点的索引
        this.i = i;               // 当前节点在该层的第几个位置
        this.min = min;           // 当前节点的最小值
        this.max = max;           // 当前节点的最大值
        this.id = id;             // 当前节点的唯一ID
        this.minDiff = minDiff;   // min值的差异
        this.maxDiff = maxDiff;   // max值的差异
        this.leftChild = leftChild;         // 左孩子节点
        this.rightChild = rightChild;       // 右孩子节点
        this.leftIndex = leftIndex;   // 左孩子的索引
        this.rightIndex = rightIndex; // 右孩子的索引
        this.parent = parent; //父亲节点
        this.isBuild = isBuild

        //双向链表
        this.preNode = null
        this.nextNode = null

        nodeCount ++

    }
}


// 定义 SegmentTree 类
class SegmentTree {
    constructor(tableName,columns,index, flagBuffer, maxNodes) {
        this.root = null;          // 根节点
        this.realDataNum = maxNodes
        //this.nodes = new Array(maxNodes).fill(null);
        this.nodes = {}
        this.table_name = tableName;   // 数据库中的表名，命令行传入
        this.columns = columns
        this.index = index
        this.flag = flagBuffer;     // 读取的 flag 二进制数组
        this.cache = null

        //
        this.minDLL = new DLL()
        this.maxDLL = new DLL()

        //存储树的最底层次的外围节点。
        this.bottonLevelDLL = new DLL()
        this.head = null

        this.nodeCount =0
    }

    // 添加节点方法
    addNode(sTime, eTime, level, index, i, min = 0, max = 0, id, 
        minDiff = null, maxDiff = null, leftChild = null, rightChild = null, leftIndex = null, rightIndex = null, parent = null, isBuild=false) {
        
        const node = new SegmentTreeNode(sTime, eTime, level, index, i, min, max, id, 
            minDiff, maxDiff, leftChild, rightChild, leftIndex, rightIndex, parent, isBuild);
        
        this.nodeCount ++ 

        if (this.root === null) {
            this.root = node;     // 如果根节点为空，则设置为根节点
        }
        //this.nodes[index] = new SegmentTreeNode(sTime, eTime, level, index, i, min, max, id, minDiff, maxDiff, leftChild, rightChild, leftIndex, rightIndex);    // 将节点添加到数组中
        return node;
    }

    // 获取所有节点
    getNodes() {
        return this.nodes;
    }
}

// 从数据库读取表 b 数据
async function readTableBFromDB(querySQL) {
    try {

        timestart('SQL.query.time');
        //console.log(querySQL)
        const result = await pool.query(querySQL);
        timeend('SQL.query.time');
        
        //timestart('rows.map');
        //let a = result.rows.map(row => [row.i, Number(row.minvd), Number(row.maxvd)]);

        let a = result.rows.map(row => Object.values(row));

        //console.log(a)

        // a = result.rows
        // console.log(a)
        //timeend('rows.map');

        return a

    } catch (error) {
        console.error('读取数据库表 b 发生错误:', error);
        //process.exit(1);
    }
}



// 从缓存读取表 b 数据
async function readTableBFromCache(querySQL, index) {
    //从数据库初始化cache

    if(treeCache == null || index == 0){
        //console.log(index)
        treeCache = await readTableBFromDB(querySQL);  // 从数据库读取表 b
        //console.log('tree.cache.length',tree.cache.length)
    }


    return treeCache

}

function readFlagz(filePath) {
    try {
        const bufferData = fs.readFileSync(filePath);

        return bufferData;
    } catch (error) {
        console.error('读取 flagz 文件时发生错误:', error);
        process.exit(1);
    }
}

// CompletedNode 类
class CompletedNode {
    constructor() {
        this.needComputeSegmentTreeNode = null;

        this.isCompletedMax = false;
        this.isCompletedMin = false;

        this.alternativeNodesMax = [];
        this.alternativeNodesMin = [];

        this.currentComputingNodeMax = null;
        this.currentComputingNodeMin = null;
        
    }
}

class M4 {
    constructor(){
        this.max = -Infinity
        this.min = Infinity
        this.start_time = 0
        this.end_time = 0
        this.st_v = 0
        this.et_v = 0

        //一个M4代表一个像素列
        this.innerNodes = []    //像素列内的node
        this.stNodes = []   //像素列左边界的node index
        this.etNodes = []   //像素列内右边界node index

        this.stNodeIndex = null   //像素列左边界的node index
        this.etNodeIndex = null   //像素列内右边界node index


        //跟计算有关的
        this.alternativeNodesMax = []
        this.alternativeNodesMin = [];
        this.currentComputingNodeMax = null
        this.currentComputingNodeMin = null
        this.isCompletedMax = false;
        this.isCompletedMin = false;

        //跟计算均值有关
        this.stInterval = null
        this.etInterval = null
        this.minDLL = null
        this.maxDLL = null
        this.stNodes = []
        this.etNodes = []


        //error bound 相关
        this.errorPixels = 0

    }
    
}

class SCREEN_M4 {

    constructor(experiment,datasetname,quantity,symbolName,width,height,errorBound){
        this.M4_array = []
        this.screenEnd = 0
        this.screenStart=0
        this.height=height
        this.width=width
        this.experiment = experiment
        this.datasetname = datasetname
        this.symbolName = symbolName
        this.errorBound = errorBound
        this.quantity = quantity

        this.nodeReductionRatio = 0
        this.SQLtime = 0
        this.totalTime=0

        this.buildNodeRate = 0
        this.segmentTrees = null
        this.dataReductionRatio = 0



        this.exactMax=-Infinity
        this.exactMin=Infinity
        this.avgtMax = -Infinity
        this.avgtMin = Infinity
        this.candidateMax=-Infinity
        this.candidateMin=Infinity


        this.preError = -1
        this.deltaError = 0

        this.count=0
    }
}

class Interval{
    constructor(sTime,eTime){
        this.sTime = sTime
        this.eTime = eTime
        this.nodes = []
        this.isSame = false
    }

}



function isSingleLeaf(node){
    if(node.eTime - node.sTime <1){
        return true
    }

    return false 
}

// 查询是否包含,待测试，看测试报告
function isContain(node, m4){
    //是叶子节点
    
    if(isSingleLeaf(node)){
        switch(true){
            case node.eTime < m4.start_time:
                return -1;break; //Node在M4左边；
            case node.sTime == m4.start_time:
                return -2;break;//Node与M4左边界重合
            case node.sTime > m4.start_time && node.sTime < m4.end_time:
                return -3;break;//Node在M4内部；
            case node.sTime == m4.end_time:
                return -4;break;//Node与M4右边界重合；
            case node.sTime > m4.end_time:
                return -5;break;//Node在M4右边；
            default:
                return 0;break;
        }
    }
    else{//非叶子节点
        switch(true){
            //Node完全在M4的左边；
            case node.eTime < m4.start_time:
                return 1;break;

            //Node右边界与M4左边界重合
            case node.eTime == m4.start_time:
                return 2;break;

            //Node跨过M4左边界；
            case node.sTime < m4.start_time  && node.eTime > m4.start_time :
                return 3;break;

            //Node左边界与M4左边界重合；
            case node.sTime == m4.start_time /* && node.eTime < m4.end_time */:
                return 4; break;

            //Node在M4内部；
            case node.sTime > m4.start_time && node.eTime < m4.end_time:
                return 5; break;

            //Node右边界与M4右边界重合
            case /* node.sTime > m4.start_time && */ node.eTime == m4.end_time:
                return 6; break;

            //Node跨过M4右边界；
            case /* node.sTime > m4.start_time &&*/ node.eTime > m4.end_time && node.sTime < m4.end_time:
                return 7; break;

            //Node左边界与M4右边界重合
            case node.sTime == m4.end_time:
                return 8; break;

            //Node完全在M4的右边；
            case node.sTime > m4.end_time:
                return 9; break;
            default:
                return 0;break;
        }
    }
}


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

        // 当前M4开始，是初始开始+平均每个桶分的点数，向上取整
        res[i].start_time=globalStart + Math.ceil( i * everyNum)

        // 上一个M4结尾，是下一个M4开始-1
        res[i-1].end_time = res[i].start_time - 1

    }

    //最后一个M4，以globalEnd结尾
    res[width-1].end_time=globalEnd

    return res
}


// 计算函数
function sympleCalculate(min1, max1, min2, max2, operator, destination, isLeaf) {
    if (destination === 'min') {
        switch (operator) {
            case '+':
                return Math.min(min1 + min2, min1 + max2, max1 + min2, max1 + max2);
            case '-':
                return Math.min(min1 - min2, min1 - max2, max1 - min2, max1 - max2);
            case '*':
                return Math.min(min1 * min2, min1 * max2, max1 * min2, max1 * max2);
            case '/':
                if(min2 == 0){
                    if(isLeaf){
                        return 0
                    }
                    min2 = 1
                }
                if(max2 == 0){
                    max2 =1
                    if(isLeaf){
                        return 0
                    }
                }
                return Math.min(min1 / min2, min1 / max2, max1 / min2, max1 / max2);
        }
    } else if (destination === 'max') {
        switch (operator) {
            case '+':
                return Math.max(min1 + min2, min1 + max2, max1 + min2, max1 + max2);
            case '-':
                return Math.max(min1 - min2, min1 - max2, max1 - min2, max1 - max2);
            case '*':
                return Math.max(min1 * min2, min1 * max2, max1 * min2, max1 * max2);
            case '/':
                if(min2 == 0){
                    min2 = 1
                    if(isLeaf){
                        return 0
                    }
                }
                if(max2 == 0){
                    max2 =1
                    if(isLeaf){
                        return 0
                    }
                }
                return Math.max(min1 / min2, min1 / max2, max1 / min2, max1 / max2);
        }
    }
}






//extremes是一个数组，返回一个数组，数组包含：min、max、以及extremes中在min和max之间的
function generateXs(min, extremes, max){

    let x = []

    for(let i=0;i<extremes.length;i++){
        if(extremes[i] > min && extremes[i] < max){
            x.push(extremes[i])
        }
    }

    x.push(min)
    x.push(max)

    return x
}


function calMean(computingNodes, destination){
    let sum = 0
    if (destination === 'min') {
        for(let i=0;i<computingNodes.length;i++){
            sum += computingNodes[i].min
        }
    }else{
        for(let i=0;i<computingNodes.length;i++){
            sum += computingNodes[i].max
        }
    }

    return sum/computingNodes.length
}

function getClosestVal(node, val) {
    if (val < node.min) {
        return node.min;
    } else if (val > node.max) {
        return node.max;
    }
    return val;
}

function computeMinVariance(minmaxVals) {
    const result = mergeCompute(minmaxVals, 0, minmaxVals.length);
    if (result.length === 2) { // An intersection exists across all the time series
        return 0;
    } else {
        const estimatedAvg = result[0] / minmaxVals.length;
        const minVarList = minmaxVals.map(vals => getClosestVal(vals, estimatedAvg));
        // console.log(minVarList)
        return minVarList;
    }
}

function mergeCompute(minmaxVals, start, end) {
    if (end - start === 1) {
        // console.log([minmaxVals[start].min, minmaxVals[start].max])
        return [minmaxVals[start].min, minmaxVals[start].max];
    }
    const mid = Math.floor((start + end + 1) / 2);
    const resultL = mergeCompute(minmaxVals, start, mid);
    const resultR = mergeCompute(minmaxVals, mid, end);
    
    // console.log(mid, resultL, resultR);
    
    if (resultL.length === 2 && resultR.length === 2) {
        // compute intersection
        const intersection = [Math.max(resultL[0], resultR[0]), Math.min(resultL[1], resultR[1])];
        if (intersection[0] <= intersection[1]) {
            return intersection;
        } else { // no intersection
            if (resultL[0] === intersection[0]) {
                return [intersection[0] * (mid - start) + intersection[1] * (end - mid)];
            } else {
                return [intersection[1] * (mid - start) + intersection[0] * (end - mid)];
            }
        }
    } else if (resultL.length === 1 && resultR.length === 1) {
        return [resultL[0] + resultR[0]];
    } else {
        if (resultL.length === 2) { // resultR.length === 1
            const avgR = resultR[0] / (end - mid);
            return [getClosestVal(resultL, avgR) * (mid - start) + resultR[0]];
        } else { // resultL.length === 1 and resultR.length === 2
            const avgL = resultL[0] / (mid - start);
            return [resultL[0] + getClosestVal(resultR, avgL) * (end - mid)];
        }
    }
}

function computeMaxVariance(minmaxVals) {
    let count = 1;
    let maxVarSums = [...[minmaxVals[0].min, minmaxVals[0].max]];
    // console.log(maxVarSums)
    let maxVarList = maxVarSums.map(i => [i]);
    // console.log(maxVarList)
    
    for (let vals of minmaxVals.slice(1)) {
        count++;
        for (let i = 0; i < maxVarSums.length; i++) {
            const addMin = maxVarSums[i] + vals.min;
            const addMax = maxVarSums[i] + vals.max;
            
            if (Math.abs(vals.min - addMin / count) > Math.abs(vals.max - addMax / count)) {
                maxVarSums[i] = addMin;
                maxVarList[i].push(vals.min);
            } else {
                maxVarSums[i] = addMax;
                maxVarList[i].push(vals.max);
            }
        }
    }
    //console.log(maxVarList)
    return maxVarList;
}

function sympleMean(computeData){
    let r=0
    for (let i = 0; i < computeData.length; i++) {
        r += computeData[i]
    }
    r /= computeData.length;

    return r
}

function sympleVariance(computeData){
    let mean = sympleMean(computeData)

    let r = 0
    for (let i = 0; i < computeData.length; i++) {
        r += (computeData[i] - mean) ** 2
    }

    return r /= computeData.length
}

function calVarianceEstimate(computingNodes, destination){
    if (destination === 'min') {
        let minArray =  computeMinVariance(computingNodes)
        if(minArray ===0){
            return 0
        }
        return sympleVariance(minArray)
    } else {
        let maxArray =  computeMaxVariance(computingNodes)
        return Math.max(sympleVariance(maxArray[0]), sympleVariance(maxArray[1]))
    }
}

function calVarianceExact(computingNodes, destination){
   
    let mean = calMean(computingNodes, destination)
    let sum = 0
    for (let i = 0; i < computingNodes.length; i++) {
        sum += (computingNodes[i].min - mean) ** 2
    }

    return sum/computingNodes.length
}

function calVariance(computingNodes, destination, isLeaf){
    
    if(isLeaf){
        return calVarianceExact(computingNodes, destination)
    }else{
        return calVarianceEstimate(computingNodes, destination)
    }

}

//统一的计算，既可以单条，也可以多条,trees计算对象，func计算函数，+-*/或其他复杂函数，mode：single or multi
function unifiedCalulate(trees, computingNodes, func, mode, isLeaf){

    if(computingNodes == null || computingNodes.length == 0){
        return {
            tmpmin: null,
            tmpmax: null
        }
    }

    let tmpmin = 0;
    let tmpmax = 0;
    if (func.funName == '+' || func.funName == '-' || func.funName == '*' || func.funName == '/') {
        //写一个min、max的计算的排列组合，目前用sympleCalculate暂代。
        tmpmin = sympleCalculate(
            computingNodes[0].min
            , computingNodes[0].max
            , computingNodes[1].min
            , computingNodes[1].max
            , func.funName
            , 'min', isLeaf);

        tmpmax = sympleCalculate(
            computingNodes[0].min
            , computingNodes[0].max
            , computingNodes[1].min
            , computingNodes[1].max
            , func.funName
            , 'max', isLeaf);
    } else if (func.funName == 'mean') {
        tmpmin = calMean(computingNodes, 'min')
        if(isLeaf){
            tmpmax=tmpmin
        }else{
            tmpmax = calMean(computingNodes, 'max')
        }
        // tmpmin = calMean(computingNodes, 'min')
        // tmpmax = calMean(computingNodes, 'max')
    } else if (func.funName == 'variance') {
        tmpmin = calVariance(computingNodes, 'min', isLeaf)
        if(isLeaf){
            tmpmax=tmpmin
        }else{
            tmpmax = calVariance(computingNodes, 'max', isLeaf)
        }
        // tmpmin = calVariance(computingNodes, 'min', isLeaf)
        // tmpmax = calVariance(computingNodes, 'max', isLeaf)
    }else if(func.funName == 'func4'){
        tmpmin = func.func4(computingNodes, 'min', isLeaf)
        if(isLeaf){
            tmpmax=tmpmin
        }else{
            tmpmax = func.func4(computingNodes, 'max', isLeaf)
        }
    }else if(func.funName == 'func1' || func.funName == 'boxcox_0' 
        || func.funName == 'boxcox_1_2' || func.funName == 'boxcox_1' || func.funName == 'boxcox_2'){
        
        tmpmin = func.compute(func.funName, computingNodes, 'min')
        if(isLeaf){
            tmpmax=tmpmin
        }else{
            tmpmax = func.compute(func.funName, computingNodes, 'max')
        }
    }



    
    
    
    else{
        // let Xs = generateXs(computingNodes[0].min, func.extremes, computingNodes[0].max)
        // let Ys = func.computes(Xs)
        // tmpmin = Math.min(...Ys)
        // tmpmax = Math.max(...Ys)
    }


    return {
        tmpmin: tmpmin,
        tmpmax: tmpmax
    }





    if(func.mode == 'multi'){
        //写一个min、max的计算的排列组合，目前用sympleCalculate暂代。
        tmpmin = sympleCalculate(
            computingNodes[0].min
           ,computingNodes[0].max
           ,computingNodes[1].min
           ,computingNodes[1].max
           ,func.funName,'min');

        tmpmax = sympleCalculate(
            computingNodes[0].min
           ,computingNodes[0].max
           ,computingNodes[1].min
           ,computingNodes[1].max
           ,func.funName, 'max');

        return {
            tmpmin: tmpmin,
            tmpmax: tmpmax
        }
    }else if(func.mode == 'single' || true){
        let Xs = generateXs(computingNodes[0].min, func.extremes, computingNodes[0].max)
        let Ys = func.computes(Xs)
        tmpmin = Math.min(...Ys)
        tmpmax = Math.max(...Ys)

        return {
            tmpmin: tmpmin,
            tmpmax: tmpmax
        }
    }

    
}









// 根据宽度构建树
async function buildtree(table,columns,tree_index,width, screenStart,screenEnd){
    const path = require('path');
    // 从命令行获取表名
    let tableName = table
    let flagzFileName = `${tableName}_${columns[tree_index]}.flagz`;  // 根据表名自动生成 flagz 文件名
    let flagzFilePath = path.join(__dirname, '../flags', flagzFileName);

    //console.time('readFlagz'); // 开始计时

    //const table_c = readFlagzFile(flagzFilePath);  // 读取并解析 flagz 文件
    const flagBuffer = readFlagz(flagzFilePath);
    const segmentTree = new SegmentTree(tableName,columns,tree_index, flagBuffer,MAXNODENUM); 


    segmentTree.realDataNum = flagBuffer.length

    //!!!!todo 建好tablenum表
    const querySQL2 = `SELECT dataname,datanum FROM tablenum WHERE dataName = '${tableName}';`;
    // 从数据库读取数据
    const result = await pool.query(querySQL2);
    // 如果找到了匹配的行，则将 dataNum 赋值给变量
    //console.log(querySQL2)
    //console.log(result)
    segmentTree.realDataNum = result.rows.length > 0 ? result.rows[0].datanum : flagBuffer.length;

    //console.timeEnd('readFlagz'); // 结束计时并打印结果


    if (!tableName) {
        console.error("请提供表名作为参数。");
        process.exit(1);
    }

    const level = Math.ceil(Math.log2(width))

    const max_id = 2 ** (level)-1;

    
    //const querySQL = `SELECT i,minvd,maxvd FROM ${tableName}  where i<= ${max_id} ORDER by i ASC`;
    let querySQL = 'SELECT i ' 

    for(let i=0;i<columns.length;i++){
        querySQL = `${querySQL}, minvd_${columns[i]}, maxvd_${columns[i]}`
    }
    
    querySQL = `${querySQL} FROM ${tableName}  where i<= ${max_id} ORDER by i ASC`

    //console.time('read data from DB'); // 开始计时
    //const table_b = await readTableBFromDB(querySQL);  // 从数据库读取表 b
    const table_b = await readTableBFromCache(querySQL, tree_index)
    //console.timeEnd('read data from DB'); // 结束计时并打印结果

    //console.log('table_b',table_b)
    //console.log('table_b_dd',table_b_dd)

    let current_level = [];

    const max_level = Math.floor(Math.log2(flagBuffer.length/2)) + 1;  // 树的最大层数

    // 初始化根节点
    let sTime = 0;
    let eTime = flagBuffer.length-1


    // constructor(sTime, eTime, level, index, i, min = 0, max = 0, 
    //  id, minDiff = null, maxDiff = null, leftChild = null, rightChild = null, leftIndex = null, rightIndex = null, parent = null, isBuild=null)

    //const rootNode = segmentTree.addNode(sTime, eTime, 0, 0, 0, table_b[0][1], table_b[0][2], 0, null, null);
    // const rootNode = new SegmentTreeNode(sTime, eTime, 0, 0, 0, table_b[0][1], table_b[0][2], 
    //     0, null, null,null, null, null,null, null, true);
    const rootNode = segmentTree.addNode(sTime, eTime, 0, 0, 0, table_b[0][tree_index*2+ 1], table_b[0][tree_index*2+ 2], 
        0, null, null,null, null, null,null, null, true);
    segmentTree.root = rootNode

    current_level.push(rootNode);

    
   // console.time('build tree Branches'); // 开始计时

    let cnt = 0;  // 节点ID从1开始
    // 从第二行开始遍历表b，逐层构建树，直到构建到第 n+1 层
    for (let i = 1; i < table_b.length; i++) {
        const current_diff_min = table_b[i][tree_index*2+ 1];
        const current_diff_max = table_b[i][tree_index*2+ 2];
        const parent_node = current_level.shift();



        const level = parent_node.level + 1;  // 层级是父节点层级加1
        const position_in_level = i - (2 ** level);  // 计算i值
        const left_index = 2*parent_node.index + 1;  // 左孩子索引
        const right_index = 2*parent_node.index + 2;  // 右孩子索引

        let left_node_min, right_node_min, left_node_max, right_node_max;

        if (current_diff_min === null && current_diff_max === 0) {
            left_node_min = null;
            left_node_max = null;
            right_node_min = parent_node.min;
            right_node_max = parent_node.max;
        } else if (current_diff_min === 0 && current_diff_max === null) {
            left_node_min = parent_node.min;
            left_node_max = parent_node.max;
            right_node_min = null;
            right_node_max = null;
        } else {
            if (current_diff_min <= 0) {
                left_node_min = parent_node.min;
                right_node_min = left_node_min - current_diff_min;
            } else {
                right_node_min = parent_node.min;
                left_node_min = right_node_min + current_diff_min;
                
            }

            if (current_diff_max <= 0) {
                right_node_max = parent_node.max;
                left_node_max = right_node_max + current_diff_max;
                
            } else {
                left_node_max = parent_node.max;
                right_node_max = left_node_max - current_diff_max;
            }
        }
        sTime = parent_node.sTime
        eTime = Math.floor((parent_node.eTime+parent_node.sTime)/2)
        const left_node = segmentTree.addNode(sTime, eTime, level, left_index,   null, left_node_min, left_node_max,   left_index, 
            null, null, null,null, null, null, parent_node, true);
      
        sTime = Math.floor((parent_node.eTime+parent_node.sTime)/2) + 1
        eTime = parent_node.eTime
        const right_node = segmentTree.addNode(sTime, eTime, level, right_index, null, right_node_min, right_node_max, right_index, 
            null, null, null, null, null, null, parent_node, true);

        parent_node.leftIndex = left_index;
        parent_node.rightIndex = right_index;
        parent_node.leftChild = left_node;
        parent_node.rightChild = right_node;

        if (left_node.min !== null || left_node.max !== null) current_level.push(left_node);
        if (right_node.min !== null || right_node.max !== null) current_level.push(right_node);
    }

   // console.timeEnd('build tree Branches'); // 结束计时并打印结果

 
  //  console.time('build tree Leaves'); // 开始计时

    //目前假设树足够深，width较小，因此不会构建到树的叶子层，所以用不到flag。
    if (width > flagBuffer.length / 2) {
        for (let i = 0; i < flagBuffer.length; i += 2) {
            const leftFlag = flagBuffer[i];
            const rightFlag = flagBuffer[i + 1];

            // const parentIndex = flagBuffer.length / 2 - 1 + i / 2;  // 计算对应的父节点索引
            // const parentNode = segmentTree.nodes[parentIndex];

            // 从 current_level 中获取对应的父节点
            const parentNode = current_level[i / 2];

            if (parentNode === null) {
                continue; // 跳过空的父节点
            }
            const left_index = 2 * parentNode.index + 1;
            const right_index = 2 * parentNode.index + 2;


            // 如果 leftChild 和 rightChild 都为 00
            if (leftFlag === 0 && rightFlag === 0) {

                sTime = parentNode.sTime
                eTime = Math.floor((parentNode.eTime + parentNode.sTime) / 2)

                const leftNode = segmentTree.addNode(sTime, eTime, parentNode.level + 1, left_index, null, parentNode.max, parentNode.max, 
                    left_index, null, null, null, null, null, null, parentNode, true);

                sTime = Math.floor((parentNode.eTime + parentNode.sTime) / 2) + 1
                eTime = parentNode.eTime
                const rightNode = segmentTree.addNode(sTime, eTime, parentNode.level + 1, right_index, null, parentNode.min, parentNode.min, 
                    right_index, null, null, null, null, null, null,parentNode, true);
                
                parentNode.leftChild = leftNode;
                parentNode.rightChild = rightNode;
            }
            // 如果 leftChild 和 rightChild 都为 11
            else if (leftFlag === 1 && rightFlag === 1) {

                sTime = parentNode.sTime
                eTime = Math.floor((parentNode.eTime + parentNode.sTime) / 2)

                const leftNode = segmentTree.addNode(sTime, eTime, parentNode.level + 1, left_index, null, parentNode.min, parentNode.min, 
                    left_index, null, null, null, null, null, null, parentNode, true);

                sTime = Math.floor((parentNode.eTime + parentNode.sTime) / 2) + 1
                eTime = parentNode.eTime
                const rightNode = segmentTree.addNode(sTime, eTime, parentNode.level + 1, right_index, null, parentNode.max, parentNode.max, 
                    right_index, null, null, null, null, null, null, parentNode, true);
         
                parentNode.leftChild = leftNode;
                parentNode.rightChild = rightNode;
            }
            // 如果 leftChild 为 1，rightChild 为 0
            else if (leftFlag === 1 && rightFlag === 0) {
                sTime = parentNode.sTime
                eTime = Math.floor((parentNode.eTime + parentNode.sTime) / 2)

                const leftNode = segmentTree.addNode(sTime, eTime, parentNode.level + 1, left_index, null, parentNode.min, parentNode.max, 
                    left_index, null, null, null, null, null, null, parentNode, true);
                
                parentNode.leftChild = leftNode;
                parentNode.rightChild = null; // 右子节点为空
            }
            // 如果 leftChild 为 0，rightChild 为 1
            else if (leftFlag === 0 && rightFlag === 1) {

                parentNode.leftChild = null;
                sTime = Math.floor((parentNode.eTime + parentNode.sTime) / 2) + 1
                eTime = parentNode.eTime
                const rightNode = segmentTree.addNode(sTime, eTime, parentNode.level + 1, right_index, null, parentNode.min, parentNode.max, 
                    right_index, null, null, null, null, null, null, parentNode, true);
               
                parentNode.leftChild = null; // left子节点为空
                parentNode.rightChild = rightNode;
            }
        }


    }



    let { StartIndex, EndIndex } = getTreeLastSE(segmentTree, width);
    let computeArrayIndex = [];
    for(let a = StartIndex;a<=EndIndex;a++){
        computeArrayIndex.push(a)
    }
    segmentTree.bottonLevelDLL.constructFromList(computeArrayIndex)
    //segmentTree.maxDLL.constructFromList(computeArrayIndex)

    buildDDL(segmentTree, current_level)


   // console.timeEnd('build tree Leaves'); // 结束计时并打印结果

    return segmentTree
}


function buildDDL(segmentTree, bottonLevel){

     // 遍历索引列表，构建双向链表
     for (let i = 0; i < bottonLevel.length; i++) {
        const node = bottonLevel[i];


        // 如果是第一个节点，将其设置为头节点
        if (i === 0) {
            segmentTree.head = node;
        }

        // 设置前驱和后继
        if (i > 0) {
            // 前一个节点的NextIndex指向当前节点
            let preNode = bottonLevel[i-1]
            preNode.nextNode = node;
            // 当前节点的preIndex指向前一个节点
            node.preNode = preNode
        }
    }
}

//获取树中实际的数据，注意：不是树包含的数据，因为如果对应的原始时间序列，
//不满足2的整数次幂的话，是要在结尾补null的，而我们要获取的，是原始时间序列的数据个数,即去掉结尾的null之后的个数
function getRealDataRowNum(segmentTree){
    return segmentTree.realDataNum
}

//获取树最低层的第一个节点StartIndex和最后一个节点的EndIndex
// 如果树不是满的，则需要修改代码。
//获取树最低层的第一个节点StartIndex和最后一个节点的EndIndex
function getTreeLastSE(segmentTree1, width, screenStart, screenEnd){

    const level = Math.ceil(Math.log2(width))

    const max_index = 2 ** (level + 1) - 2;
    return  {
        StartIndex: max_index / 2,
        EndIndex: max_index
    };
}

async function getTableBFromDB(segmentTree, indexset){
    

    let querySQL, table_b

    //timestart('nomerge');

    //querySQL = `SELECT i, minvd, maxvd FROM ${segmentTree.table_name} where i in (`;

    querySQL = 'SELECT i ' 

    for(let i=0;i<segmentTree.columns.length;i++){
        querySQL = `${querySQL}, minvd_${segmentTree.columns[i]}, maxvd_${segmentTree.columns[i]}`
    }

    querySQL = `${querySQL} FROM ${segmentTree.table_name}  where i in (`


    for(let a = 0; a < indexset.length - 1; a++){
        querySQL = querySQL.concat(`${indexset[a]}, `);
    }
    querySQL = querySQL.concat(`${indexset[indexset.length - 1]});`);

    table_b = await readTableBFromCache(querySQL,segmentTree.index);

    //let nomergeSpent =  timeend('nomerge');


    // timestart('merge');

    // let WhereCondition = mergeIntervalsForWhereCondition(indexset)
    // //let querySQL = `SELECT i, minvd, maxvd FROM ${segmentTree.table_name} ${WhereCondition} ORDER BY i ASC ;`;
    // querySQL = `SELECT i, minvd, maxvd FROM ${segmentTree.table_name} ${WhereCondition} ;`;
    // table_b = await readTableBFromDB(querySQL);

    // let mergeSpent =  timeend('merge');


    
    // if(mergeSpent > nomergeSpent){
    //     //console.log(querySQL,'\n')
    //     console.log((mergeSpent-nomergeSpent).toFixed(2))
    // }

    //console.log('sql length:',querySQL.length,'\n')

    return table_b

}


function buildNode(node, segmentTree, tableb_map, i){

    let parent_node = node.parent
    let min = parent_node.min;
    let max = parent_node.max;
    let level = parent_node.level;
    let parent_index = parent_node.index

    let index = node.index
    //是否有优化空间，可以不用算
    let {sTime, eTime} = getSETimeByIndex(segmentTree, index);
    node.sTime = sTime
    node.eTime = eTime





    if (!isSingleLeaf(node)) {

//console.log(parent_index)

        let current_diff_min = tableb_map.get(parent_index + 1)[1 + i*2];
        let current_diff_max = tableb_map.get(parent_index + 1)[2 + i*2];

        if (isLeftNode(index)) {
            if (current_diff_min < 0) { }
            else {
                min = min + current_diff_min;
            }

            if (current_diff_max < 0) {
                max = max + current_diff_max;
            } else { }

            //i = 2 * i;
        } else {
            if (current_diff_min < 0) {
                min = min - current_diff_min;
            } else { }

            if (current_diff_max < 0) { }
            else {
                max = max - current_diff_max;
            }

            //i = 2 * i + 1;
        }

        node.level = level + 1
        node.min = min
        node.max = max
        node.isBuild = true
    } else {
        let flag = readFlag(segmentTree, index);
        if (flag[0] == 1 && flag[1] == 0) {
            //i = 2 * i;
        } else if (flag[0] == 0 && flag[1] == 1) {
            //i = 2 * i + 1;
        } else if (flag[0] == 0 && flag[1] == 0) {
            if (isLeftNode(index)) {
                min = max;
                //i = 2 * i;
            } else {
                max = min;
                //i = 2 * i + 1;
            }
        } else if (flag[0] == 1 && flag[1] == 1) {
            if (isLeftNode(index)) {
                max = min;
                //i = 2 * i;
            } else {
                min = max;
                //i = 2 * i + 1;
            }
        }

        node.level = level + 1
        node.min = min
        node.max = max
        node.isBuild = true
    }
}

async function Query(needQueryNodes, leaves, segmentTree){
    
     let indexset = [];
     let indexSort = []

    needQueryNodes.sort(function(a, b){return a.index - b.index});





     let parents = []

    //!!!!!todo 待均值计算时再改
    // for(let i=0;i<leaves.length;i++){
    //     let index = leaves[i]
    //     //let parent_index = parentIndex(index);
    //     //如果父节点是空，则需要将其父节点及其祖宗一起查询并构建。
    //     if(segmentTree.nodes[index]==null && !indexset.has(index)){
    //         let route = findRoute(segmentTree, index, indexset);
    //         parents.push(...route)
    //     }
    // }


    if(parents.length>0){
        parents.forEach(item => indexset.add(item));
    }
    if(leaves.length >0){
        leaves.forEach(item => indexset.add(item));
    }

    
    for(let i=0;i<needQueryNodes.length;i++){
        indexset.push(parentIndex(needQueryNodes[i].index)+1)
    }


    //console.log(indexset)
    //去重
    indexset = Array.from(new Set(indexset));
    //console.log(indexset)

    // for(let n=0;n<indexset.length;n++){
    //     console.log(indexset[n])
    // }
    //     let intervals = mergeIntervals(indexset)
    //     //console.log(indexset)
    //     console.log(intervals)
    //  //


 

    //let table_b = await getTableBFromCache(segmentTree,indexArray)
    let table_b = await getTableBFromDB(segmentTree,indexset)


    let tableb_map = new Map();
    let tree_index = segmentTree.index
    table_b.forEach(e =>{
        tableb_map.set(e[0], [e[0], e[tree_index*2+ 1], e[tree_index*2+ 2]]);
    })


    for(let i=0;i<needQueryNodes.length;i++){

        let node = needQueryNodes[i]
        if(node.isBuild){
            continue
        }

        buildNode(node, segmentTree, tableb_map, 0)
    }
}

//根据indexArray，从数据库中查询到相应的信息，并计算出相应的树节点，分别补充到SegmentTrees中
async function Multi_Query(needQueryNodesTrees,leaves, segmentTrees){

    //timestart('Multi_Query');

    let needQueryNodesTrees2 = new Array(segmentTrees.length)
    for (let i = 0; i < needQueryNodesTrees2.length; i++) {
        needQueryNodesTrees2[i] = [];
    }


    for(let i=0;i<needQueryNodesTrees.length;i++){
        for(let j=0;j<needQueryNodesTrees[i].length;j++){
            let node = needQueryNodesTrees[i][j]
            if(node.isBuild == false){
                needQueryNodesTrees2[i].push(node)
            }
        }

    }
    if(needQueryNodesTrees2[0].length == 0 && leaves.length == 0 ){
        return
    }


    //timestart('Query');
    for(let i = 0; i < needQueryNodesTrees2.length; i++){
        await Query(needQueryNodesTrees2[i], leaves, segmentTrees[i]);
    }
    //timeend('Query');


    // timestart('QueryWithJoin');
    // await QueryWithJoin(needQueryNodesTrees2, leaves, segmentTrees);
    // timeend('QueryWithJoin');


    //timeend('Multi_Query');

}


function getChildren(segmentTree1, index){
    let { leftIndex, rightIndex } = getChildrenIndex(index);
    let leftChild = segmentTree1.addNode()
    let { sTime:sTime1, eTime:eTime1 } = getSETimeByIndex(segmentTree1, leftIndex);
    leftChild.sTime = sTime1
    leftChild.eTime = eTime1
    leftChild.index = leftIndex

    let rightChild = segmentTree1.addNode()
    let { sTime:sTime2, eTime:eTime2 } = getSETimeByIndex(segmentTree1, rightIndex);
    rightChild.sTime = sTime2
    rightChild.eTime = eTime2
    rightChild.index = rightIndex

    return {
        leftChild:leftChild,
        rightChild:rightChild
    }
}



function getChildrenIndex(index){

    return {
        leftIndex: 2 * index + 1,
        rightIndex: 2 * index + 2
    };

    
    return {
        leftIndex: 0,
        rightIndex: 63
    };
    
}


//根据segmentTree的结构，返回index节点的sTime和eTime
function getSETimeByIndex(segmentTree, index){
    let srange = segmentTree.root.sTime;
    let erange = segmentTree.root.eTime;
    let level = Math.floor(Math.log2(index + 1));
    let i = index - 2 ** level + 1; 
    let interval = (erange - srange + 1) / (2 ** level);
    let sTime = srange + i * interval;
    let eTime = sTime + interval - 1;
    return  {
        sTime: sTime,
        eTime: eTime
    };
}


// 判断是否为叶子节点
function isLeafNode(segmentTree, index){
    return index >= segmentTree.flag.length - 1;
}

// 寻找叶子节点的位置
function getPosition(segmentTree, index){
    if(isLeafNode(segmentTree, index)){
        return index - segmentTree.flag.length + 1;
    } else{
        console.log(`This node whose index = ${index} is not a leafnode.`);
        return -1;
    }
}

// 从 buffer 里读某一个叶子节点对应表c数据
function readFlag(segmentTree, index){
    if(isLeafNode(segmentTree, index)){
        let position = getPosition(segmentTree, index);
        if(segmentTree.flag[position] == null){
            return null;
        } else if(isLeftNode(index)){
            return [segmentTree.flag[position], segmentTree.flag[position + 1]];
        } else{
            return [segmentTree.flag[position - 1], segmentTree.flag[position]];
        }
    } else{
        console.log(`This node whose index = ${index} is not a leafnode.`);
        return -1;
    }
}

// 获得父节点
function parentIndex(index){
    if(!isLeftNode(index)){
        return (index - 2) / 2;
    } else{
        return (index - 1) / 2;
    }
}


// 判断节点为左或右子节点
function isLeftNode(index){
    return index % 2 != 0;
}





//构造一个双向链表，节点信息：{ownIndex, preIndex, NextIndex}
// 以字典方式存储，key是ownIndex，value是上面的三元组
//支持操作：addPre(index,pre)，在index前增加，addNext(index, next),在index后增加；
//delete（index）,getPre(index), getNext(index)
// 节点类，表示链表中的每个节点
class Node {
    constructor(ownIndex, preIndex = null, NextIndex = null) {
        this.ownIndex = ownIndex;
        this.preIndex = preIndex;
        this.NextIndex = NextIndex;
    }
}

// 双向链表类，包含节点操作
class DLL {
    constructor() {
        this.nodes = {}; // 存储节点的字典，键为节点的ownIndex
        this.head = null; // 链表的头节点
    }

    // 构建双向链表的方法，从传入的索引列表创建链表
    constructFromList(indexList) {

        // 清空现有的链表
        this.nodes = {};

        // 遍历索引列表，构建双向链表
        for (let i = 0; i < indexList.length; i++) {
            const currentIndex = indexList[i];


            // 创建当前节点并添加到字典
            const newNode = new Node(currentIndex);
            this.nodes[currentIndex] = newNode;

            // 如果是第一个节点，将其设置为头节点
            if (i === 0) {
                this.head = newNode;
            }

            // 设置前驱和后继
            if (i > 0) {
                // 前一个节点的NextIndex指向当前节点
                this.nodes[indexList[i - 1]].NextIndex = currentIndex;
                // 当前节点的preIndex指向前一个节点
                this.nodes[currentIndex].preIndex = indexList[i - 1];
            }
        }
    }

    parentToChildren(index,leftChild,rightChild){

        this.addPre(index, leftChild)
        this.addNext(index, rightChild)

        // 删除当前节点
        this.delete(index);
    }

    // 删除指定的节点，并在其前后插入新节点
    deleteAndInsert(index, pre, next) {
        const currentNode = this.nodes[index];
        if (!currentNode) {
            console.error("No node exists at index " + index);
            return;
        }

        // 获取要删除节点的前驱和后继节点
        const preNode = this.nodes[currentNode.preIndex];
        const nextNode = this.nodes[currentNode.NextIndex];

        // 删除当前节点
        this.delete(index);

        // 插入新的前驱节点（在原来的前驱和原来的nextNode之间）
        const newPreNode = new Node(pre, currentNode.preIndex, next);
        this.nodes[pre] = newPreNode;
        if (preNode) {
            preNode.NextIndex = pre;
        }
        if (nextNode) {
            nextNode.preIndex = pre;
        }

        // 插入新的后继节点（在新插入的pre和原来的nextNode之间）
        const newNextNode = new Node(next, pre, currentNode.NextIndex);
        this.nodes[next] = newNextNode;
        newPreNode.NextIndex = next;
        if (nextNode) {
            nextNode.preIndex = next;
        }
    }

    // 在给定索引的节点前添加一个新的节点
    addPre(index, pre) {
        if (this.nodes[index]) {
            const currentNode = this.nodes[index];
            const newNode = new Node(pre, currentNode.preIndex, index);

            // 更新现有前驱节点的NextIndex
            if (this.nodes[currentNode.preIndex]) {
                this.nodes[currentNode.preIndex].NextIndex = pre;
            }else {
                // 如果当前节点是头节点，将新节点设置为头节点
                this.head = newNode;
            }

            // 更新当前节点的preIndex
            currentNode.preIndex = pre;

            // 将新节点添加到字典
            this.nodes[pre] = newNode;
        } else {
            console.error("No node exists at index " + index);
        }
    }

    // 在给定索引的节点后添加一个新的节点
    addNext(index, next) {
        if (this.nodes[index]) {
            const currentNode = this.nodes[index];
            const newNode = new Node(next, index, currentNode.NextIndex);

            // 更新现有后继节点的preIndex
            if (this.nodes[currentNode.NextIndex]) {
                this.nodes[currentNode.NextIndex].preIndex = next;
            }

            // 更新当前节点的NextIndex
            currentNode.NextIndex = next;

            // 将新节点添加到字典
            this.nodes[next] = newNode;
        } else {
            console.error("No node exists at index " + index);
        }
    }

    // 删除指定索引的节点
    delete(index) {
        const currentNode = this.nodes[index];
        if (!currentNode) {
            console.error("No node exists at index " + index);
            return;
        }

        // 更新前驱和后继节点的连接
        if (this.nodes[currentNode.preIndex]) {
            this.nodes[currentNode.preIndex].NextIndex = currentNode.NextIndex;
        }else {
            // 如果要删除的是头节点，将下一个节点设置为头节点
            this.head = this.nodes[currentNode.NextIndex] || null;
        }

        if (this.nodes[currentNode.NextIndex]) {
            this.nodes[currentNode.NextIndex].preIndex = currentNode.preIndex;
        }

        // 从字典中删除当前节点
        delete this.nodes[index];
    }


    getOwn(index){
        return this.nodes[index];
    }


    // 获取给定索引的前驱节点
    getPre(index) {
        if (this.nodes[index]) {
            return this.nodes[this.nodes[index].preIndex] || null;
        } else {
            return null;
        }
    }

    // 获取给定索引的后继节点
    getNext(index) {
        if (this.nodes[index]) {
            return this.nodes[this.nodes[index].NextIndex] || null;
        } else {
            return null;
        }
    }
}




function getMidIndex(array){
    return (array.length % 2 == 0)? array.length / 2 : (array.length - 1) / 2;
}



function getExtremeInInterval(min, max) {
    let min_pre_k = Math.ceil(min / Math.PI - 0.5);
    let max_pre_k = Math.floor(max / Math.PI - 0.5);

    let extreme = []
    for (let k = min_pre_k; k <= max_pre_k; k++) {
        extreme.push((k+0.5)*Math.PI);
    }

    return extreme;
}


class FunInfo{
    constructor(funName, extremes){
        this.funName = funName;
        this.intervalRange = 0
        this.extremes = []
        this.mode = 'multi'
        if(extremes != null){
            this.extremes.push(...extremes)
        }


        //字典，key为函数名，如func1、func2_0；value为对应的极值点数组。对x函数，取[0],对x和y函数，分别[0]和[1]
        this.maxExtremes = {}
        this.minExtremes = {}

        this.init()
    };

    init(){

        this.maxExtremes = {}
        this.minExtremes = {}

        //初始化func1、func2_0等的极值点。
        this.maxExtremes['func1'] = []
        this.minExtremes['func1'] = []
        this.maxExtremes['func1'].push(-31.62)
        this.minExtremes['func1'].push(31.62)

        this.maxExtremes['boxcox_0'] = []
        this.minExtremes['boxcox_0'] = []

        this.maxExtremes['boxcox_1_2'] = []
        this.minExtremes['boxcox_1_2'] = []

        this.maxExtremes['boxcox_1'] = []
        this.minExtremes['boxcox_1'] = []

        this.maxExtremes['boxcox_2'] = []
        this.minExtremes['boxcox_2'] = []
        this.minExtremes['boxcox_2'].push(0)

        this.maxExtremes['func3'] = []
        this.minExtremes['func3'] = []

        this.maxExtremes['func4'] = []
        this.minExtremes['func4'] = []
        //todo  初始化其他函数的极值点。
        
    }

    func4(nodes, destination, isLeaf) {
        // 函数: (1 - sin x)(ln(|y|+1))^2
    
        if(isLeaf){
            let x = nodes[0].min
            let y=nodes[1].min
            return (1 - Math.sin(x)) * (Math.log(Math.abs(y)+1)) **2
        }
    
        const xRange = nodes[0];
        const yRange = nodes[1];
        //console.log(`xRange: ${Math.sin(xRange.min)}, ${Math.sin(xRange.max)}, yRange: ${yRange.min}, ${yRange.max}`)
    
        let x_min=Infinity, x_max=-Infinity, y_min, y_max = 0;
        if (xRange.max - xRange.min >= (2 * Math.PI)) {
            x_min = -1;
            x_max = 1;
        } else {
    
            let extreme = getExtremeInInterval(xRange.min, xRange.max);
            extreme.push(xRange.min)
            extreme.push(xRange.max)
            for (let i = 0; i < extreme.length; i++) {
                let tmp = Math.sin(extreme[i]);
            
                if(x_min>tmp){
                    x_min=tmp
                }
                if(x_max<tmp){
                    x_max=tmp
                }
            }
    
        }
    
    
        y_max = Math.max(Math.abs(yRange.min), Math.abs(yRange.max));
        if (yRange.min * yRange.max < 0) {
            y_min = 0;
        } else {
            y_min = Math.min(Math.abs(yRange.min), Math.abs(yRange.max));
        }
       // console.log(`x_min = ${x_min}\nx_max = ${x_max}\ny_min = ${y_min}\ny_max = ${y_max}\n`)
    
        if (destination === 'min') {
            return (1 - x_max) * (Math.log(y_min + 1)) ** 2;
        } else {
            return (1 - x_min) * (Math.log(y_max + 1)) ** 2;
        }
    }

    //所有的nodes全是数组，只是对x函数，取nodes[0];对x和y函数，分别nodes[0]和nodes[1]
    compute(funName, nodes, destination){
 
        if(destination == 'min'){
            let Xs = this.generateXs(nodes[0].min, this.minExtremes[funName], nodes[0].max)
            let min = Infinity

            for(let i=0;i<Xs.length;i++){
                let x = Xs[i]
                let y= this.getFunc(funName, x)

                if(min > y){
                    min = y
                }
            }

            return min
        }else{
            let Xs = this.generateXs(nodes[0].min, this.maxExtremes[funName], nodes[0].max)
            let max = -Infinity

            for(let i=0;i<Xs.length;i++){
                let x = Xs[i]
                let y= this.getFunc(funName, x)

                if(max < y){
                    max = y
                }
            }

            return max
        }
    }

    getFunc(funName, x){
        if(funName == 'func1'){
            return this.func1(x)
        }else if(funName == 'boxcox_0'){
            return this.boxcox_0(x)
        }else if(funName == 'boxcox_1_2'){
            return this.boxcox_1_2(x)
        }else if(funName == 'boxcox_1'){
            return this.boxcox_1(x)
        }else if(funName == 'boxcox_2'){
            return this.boxcox_2(x)
        }


        return this.compute_defult(x) 
    }

    func1(x){
        //0.001*x^3 - 3*x
        let y = 0.001*(x**3) - 3*x

        return y
    }

    boxcox_0(x){
        //Box-Cox,lambda = 0
        //logx
        if(x<=0){
            return 0
        }

        return Math.log(x)
    }
    
    boxcox_1_2(x){
        //Box-Cox,lambda =1/2
        //(x**(1/2) - 1)/(1/2)
        if(x<=0){
            x=0
        }

        return (x**(1/2) - 1)/(1/2)
    }
   
    boxcox_1(x){
        //Box-Cox,lambda = 1
        //x-1

        return x-1
    }
    
    boxcox_2(x){
        //Box-Cox,lambda = 2
        //(x**2 - 1)/2

        return (x**2 - 1)/2
    }

    
   


    // func1(nodes, destination){
    //     //0.001*x^3 - 3*x

    //     if(destination == 'min'){
    //         let Xs = this.generateXs(nodes[0].min, this.minExtremes['func1'], nodes[0].max)
    //         let min = Infinity

    //         for(let i=0;i<Xs.length;i++){
    //             let x = Xs[i]
    //             let y= 0.001*(x**3) - 3*x

    //             if(min > y){
    //                 min = y
    //             }
    //         }

    //         return min
    //     }else{
    //         let Xs = this.generateXs(nodes[0].min, this.maxExtremes['func1'], nodes[0].max)
    //         let max = -Infinity

    //         for(let i=0;i<Xs.length;i++){
    //             let x = Xs[i]
    //             let y= 0.001*(x**3) - 3*x

    //             if(max < y){
    //                 max = y
    //             }
    //         }

    //         return max
    //     }
    // }

    // func2_0(nodes, destination){
    //     //Box-Cox,lambda = 0
    // }

    // func2_1_2(nodes, destination){
    //     //Box-Cox,lambda =  1/2
    // }

    // func2_1(nodes, destination){
    //     //Box-Cox,lambda = 1
    // }

    // func2_2(nodes, destination){
    //     //Box-Cox,lambda = 2
    // }


    // func3(nodes, destination){
    //     //x^(1/(|y|+1))
    // }

    // func4(nodes, destination){
    //     //(1-sin x)(ln(|y|+1))^2
    // }

    func_variance(nodes, destination){
        //方差，若干个node，数量不固定，计算方差
    }



    //extremes是一个数组，返回一个数组，数组包含：min、max、以及extremes中在min和max之间的
    generateXs(min, extremes, max){

        let x = []

        if(extremes != null){
            for(let i=0;i<extremes.length;i++){
                if(extremes[i] > min && extremes[i] < max){
                    x.push(extremes[i])
                }
            }
        }

        x.push(min)
        x.push(max)

        return x
    }

    compute_defult(x){
        let y = x

        return y
    }
    //根据funName函数体，依次计算Xs的函数值，返回Ys数组
    computes_defult(Xs){

        let Ys=[]
        for(let i=0;i<Xs.length;i++)
        {
            //todo
            //Ys.push(sin()+cos()+....)
            let x=Xs[i]
            let y=this.compute_defult(x)

            Ys.push(y)
        }
        
        return Ys
    }

}




async function fenlie(M4_array, screenStart,screenEnd, segmentTrees, func, leaves) {
    

    let currentNodes = []
    for(let i=0;i<segmentTrees.length;i++){
        currentNodes.push(segmentTrees[i].head)
    }

    if(currentNodes.length == 0){
        //error
        return
    }

    
    let needQueryNodesTrees = new Array(segmentTrees.length)
    for (let i = 0; i < needQueryNodesTrees.length; i++) {
        needQueryNodesTrees[i] = [];
    }
 
    let i = 0;
    while(i < M4_array.length && currentNodes[0]!=null){

        let m4 = M4_array[i]
        let type = isContain(currentNodes[0], m4);


        //对叶子结点
        if(type == -1){
            for(let i=0;i<segmentTrees.length;i++){
                currentNodes[i] = currentNodes[i].nextNode
            }

            continue;
        }

        //叶子Node与M4左边界重合，该节点的值（因为是叶子节点，所以min=max）赋给该M4的左边界st_v
        if(type == -2){
            //m4.stNodeIndex=node.index   
            for(let i=0;i<segmentTrees.length;i++){
                m4.stNodes.push(currentNodes[i])
                currentNodes[i] = currentNodes[i].nextNode
            }

            continue;
        }

        //叶子Node在M4内部，放到该M4的inner中
        if (type == -3) {
            //m4.innerNodes.push(node.index)
            let nodePairs = []
            for(let i=0;i<segmentTrees.length;i++){
                nodePairs.push(currentNodes[i])
                currentNodes[i] = currentNodes[i].nextNode
            }
            m4.innerNodes.push(nodePairs)
            continue;
        }

        //叶子Node与M4右边界重合，该节点的值（因为是叶子节点，所以min=max）赋给该M4的右边界et_v
        if (type == -4) {
            //m4.etNodeIndex = node.index
            for(let i=0;i<segmentTrees.length;i++){
                m4.etNodes.push(currentNodes[i])
                currentNodes[i] = currentNodes[i].nextNode
            }
            continue;
        }
        if(type == -5){
            i++
            continue;
        }

        if(type == 1){
            //cuttentNode = segmentTrees[0].bottonLevelDLL.getNext(cuttentNode.ownIndex)
            for(let i=0;i<segmentTrees.length;i++){
                currentNodes[i] = currentNodes[i].nextNode
            }
            continue;
        }

        //要进行分裂
        if(type == 2 || type == 3 || type == 4 || type == 6 || type == 7 || type == 8){


             // 对非叶子节点，分裂其左右孩子
             for(let i=0;i<segmentTrees.length;i++){
                let{leftChild, rightChild} = getChildren(segmentTrees[i],currentNodes[i].index)

                needQueryNodesTrees[i].push(leftChild)
                needQueryNodesTrees[i].push(rightChild)

                currentNodes[i].leftChild = leftChild
                currentNodes[i].rightChild = rightChild
                leftChild.parent = currentNodes[i]
                rightChild.parent = currentNodes[i]

                //更新双向链表
                leftChild.preNode = currentNodes[i].preNode
                leftChild.nextNode = rightChild
                rightChild.preNode = leftChild
                rightChild.nextNode = currentNodes[i].nextNode

                if(segmentTrees[i].head.index == currentNodes[i].index){
                    segmentTrees[i].head = leftChild
                }

                currentNodes[i] = leftChild
            }
            
            //!!!!!!!todo
            if(func.funName == 'avg_w'){
                // segmentTrees[0].minDLL.parentToChildren(node.index,leftIndex, rightIndex)
                // segmentTrees[0].maxDLL.parentToChildren(node.index,leftIndex, rightIndex)
            }

            continue;
        }




        // 对非叶子节点，如果该node完全包含在M4内部，则不需要分裂，而是仅仅将该node加入到M4的innerNodes中即可。
        if(type == 5){
            //注意一下，对这种innerNodes的处理，在division外部已经处理了，看一下是否会处理重复。
            // m4.innerNodes.push(node.index)
            // cuttentNode = segmentTrees[0].bottonLevelDLL.getNext(cuttentNode.ownIndex)

            let nodePairs = []
            for(let i=0;i<segmentTrees.length;i++){
                nodePairs.push(currentNodes[i])
                currentNodes[i] = currentNodes[i].nextNode
            }
            m4.innerNodes.push(nodePairs)
            continue;
        }

        if (type === 9) {
            i++;
            continue;
        }

    }

    //对computeArrayUnqueryIndex进行查询，并加到computeArray中。
    let tempArrayIndex = await Multi_Query(needQueryNodesTrees, leaves, segmentTrees);
}


function outputM4(screen_m4){
    let M4_array=screen_m4.M4_array  , MIN=screen_m4.exactMin, MAX=screen_m4.exactMax, segmentTrees=screen_m4.segmentTrees

    // for(let key in stats.callCounts){
    //     timetotal(key)
    // }

    let SQLtime = getTotalTime('SQL.query.time')
    let totalTime = getTotalTime('totaltime')

    if(isNaN(SQLtime)){
        SQLtime = 0
    }

    console.log('totaltime:', totalTime.toFixed(3)+'s ,', 'sqltime:', SQLtime.toFixed(3)+'s')


    if (segmentTrees != null) {
        console.log('tree num:', segmentTrees.length
            , 'realDataNum:', segmentTrees[0].realDataNum
            , 'totalNodeNum:', segmentTrees[0].realDataNum * 2 - 1
            , 'buildNodeNum:', segmentTrees[0].nodeCount
            , 'buildNodeRate:', (segmentTrees[0].nodeCount / (segmentTrees[0].realDataNum * 2 - 1)).toFixed(4))
    }

    //return

    //console.log( 'MIN:',  MIN, 'MAX:',MAX)
    

    console.log('m4 info, experiment:',screen_m4.experiment
        , ',dataReductionRatio:', screen_m4.dataReductionRatio.toFixed(5)
        , ',table:',screen_m4.datasetname
        , ',symbol:',screen_m4.symbolName
        , ',width:',screen_m4.width
        , ',height:',screen_m4.height
        , ',errorBound:',screen_m4.errorBound)




    for(let i=0;i<M4_array.length;i++){
        let m4 = M4_array[i]
        console.log(
            'm4:',i
            ,'sT:',m4.start_time
            , ',eT:',m4.end_time
            , ',sV:',m4.st_v.toFixed(3)
            , ',eV:',m4.et_v.toFixed(3)
            , ',min:',m4.min.toFixed(3)
            , ',max:',m4.max.toFixed(3)
         )
    }



}

function mergeTables(table_name, columns, funInfo, tableType){
    let order = []
    order.push(`v${columns[0]}`)
    for(let i=1;i<columns.length;i++){
        if(columns[i] == '' || columns[i] == null){
            continue
        }

        if(funInfo.funName == 'func1' || funInfo.funName == 'boxcox_0' 
            || funInfo.funName == 'boxcox_1_2' || funInfo.funName == 'boxcox_1' || funInfo.funName == 'boxcox_2'){
             //一元计算，table_name_others不取
             if(order.length >0){
                //二元计算，table_name_others只取1个
                break
            }

        }else if(funInfo.funName == '+' || funInfo.funName == '-' || funInfo.funName == '*' || funInfo.funName == '/'
             || funInfo.funName == 'func3' || funInfo.funName == 'func4' ){
            if(order.length >1){
                //二元计算，table_name_others只取1个
                break
            }
        }

        order.push(`v${columns[i]}`)
    }
    // if(table_name_others.length > 0){
    //     tables.push(...table_name_others)
    // }


    if(tableType == 'om3'){
 
        if(!table_name.endsWith('_om3')){
            table_name =  table_name + '_om3';
        }

    }else if(tableType == 'tv'){
        if(table_name.endsWith('_om3')){
            table_name = removeEndChar(table_name, '_om3');
        }
    }

    columns.splice(0, columns.length, ...order);

    return table_name

}


async function profile(func, ...params) {

    let start = performance.now();

    //console.time(func.name); // 开始计时
    const result = await func(...params); // 执行函数
    //console.timeEnd(func.name); // 结束计时并打印结果

let end = performance.now();

console.log('start:', ((end - start) / 1000).toFixed(3)+'s')

    return result;
}
 


async function start(){

    timestart('totaltime')
    // await pool.connect()

    const args = process.argv.slice();

    let table_name=args[2]
    let columns=args[3].split(',')
    let symbol = args[4]
    let mode = args[5] //multi or single
    let width= Number(args[6])
    let height = Number(args[7])

    


    let startTime = args[8]
    let endTime = args[9]
    let interact_type = args[10]

    let experiment = args[11]
    let parallel = Number(args[12])
    let errorBound = Number(args[13])

    let datasetname = table_name.split('_') [0]

    let symbolName = symbol.split(';') [0]

    if(symbolName == '+'){
        symbolName='plus'
    }else if(symbolName == '-'){
        symbolName='minus'
    }else if(symbolName == '*'){
        symbolName='multi'
    }else if(symbolName == '/'){
        symbolName='devide'
    }else if(symbolName == 'boxcox_0'){
        symbolName='boxcox0'
    }else if(symbolName == 'boxcox_1_2'){
        symbolName='boxcox12'
    }else if(symbolName == 'boxcox_1'){
        symbolName='boxcox1'
    }else if(symbolName == 'boxcox_2'){
        symbolName='boxcox2'
    }



    let screen_m4 = new SCREEN_M4(experiment,datasetname,0,symbolName,width,height,errorBound)

    // console.log('m4 info, experiment:',experiment
    //     , ',table:',datasetname
    //     , ',quantity:',endTime-startTime+1
    //     , ',symbol:',symbolName
    //     , ',width:',width
    //     , ',height:',height
    //     , ',errorBound:',errorBound)


    // let table1=args[2]
    // let table2=args[3]
    // let symble = args[4]
    // let extremes = null
    // if(args[5] != ''){
    //     extremes = args[5].split(",").map(Number);
    // }
    // let width= Number(args[6])
    // let height = Number(args[7])
    // let experiment = args[8]
    // let mode = args[9] //multi or single
    // let screenStart = Number(args[10])
    // let screenEnd = Number(args[11])
    // let parallel = Number(args[12])
    // let errorBound = Number(args[13])



    //console.log(table_name1,table_name_others,symbol,'',width,height,mode,parallel,errorBound,startTime,endTime, interact_type)

    switch(experiment){
        case 'case0':
            startTime = Number(args[8])
            endTime = Number(args[9])
            await Case0(table_name,columns,symbol,'',width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4)
            break; 
        case 'case1':
            startTime = Number(args[8])
            endTime = Number(args[9])
            await Case1(table_name,columns,symbol,'',width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4)
            ;break;
        case 'case2':
            startTime = Number(args[8])
            endTime = Number(args[9])
            await Case2(table_name,columns,symbol,'',width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4)
            ;break;
        case 'case3':
            startTime = Number(args[8])
            endTime = Number(args[9])
            await Case3(table_name,columns,symbol,'',width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4)
            ;break;
        case 'case4':
            startTime = Number(args[8])
            endTime = Number(args[9])
            await Case4(table_name,columns,symbol,'',width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4)
            ;break;
        case 'case5':
            await Case5(table_name,columns,symbol,'',width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4)
            ;break;
            case 'case6':
                await Case6(table_name,columns,symbol,'',width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4)
                ;break;
        

        case 'test':
            await Case6_test(table_name,columns,symbol,'',width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4);
            break;
    }

    timeend('totaltime');
// console.log(' pool.end()')
//     pool.end()
    pool.end()


    outputM4(screen_m4)



}





const stats = {
    functionTimes: {},
    startTimes: {},
    callCounts: {}  // 新增用于记录调用次数
};

// 开始计时函数
function timestart(functionName) {
    stats.startTimes[functionName] = performance.now();
}

// 结束计时函数
function timeend(functionName) {
    if (!stats.startTimes[functionName]) {
        console.error(`No start time recorded for ${functionName}`);
        return;
    }

    const startTime = stats.startTimes[functionName];
    const endTime = performance.now();
    const timeSpent = endTime - startTime;

    // 更新总时间
    if (!stats.functionTimes[functionName]) {
        stats.functionTimes[functionName] = 0;
    }
    stats.functionTimes[functionName] += timeSpent;

    // 更新调用次数
    if (!stats.callCounts[functionName]) {
        stats.callCounts[functionName] = 0;
    }
    stats.callCounts[functionName]++;

    // 清除开始时间
    delete stats.startTimes[functionName];


    return timeSpent
}

function getTotalTime(functionName){
    return (stats.functionTimes[functionName]) / 1000
}






// 使用profile函数计算并打印slowFunction的执行时间
 profile( start);



 //====================以下都是实验代码================
 //test()
 //test_computeM4TimeSE()



function generateM4(result, width, startTime, endTime){

    let res = computeM4TimeSE(width, [startTime, endTime]);
    //console.log(res)
    let MIN = Infinity;
    let MAX = -Infinity;
    let difference = startTime;
    res.forEach(e =>{
        let min = Infinity;
        let max = -Infinity;
        for(let i = e.start_time; i <= e.end_time; i++){
            if(result[i - difference].v < min){
                min = result[i - difference].v
            }

            if(result[i - difference].v > max){
                max = result[i - difference].v
            }
        }
        e.min = min
        e.max = max
        e.st_v = result[e.start_time - difference].v
        e.et_v = result[e.end_time - difference].v
        // 更新MIN,MAX
        if (MIN > min) { MIN = min; }
        if (MAX < max) { MAX = max; }       
    })

    return {
        M4_array: res,
        min_value: MIN,
        max_value: MAX
    }
}



function compute(computeData, funInfo, interact_type){
    if(interact_type == 'only_show'){
        return computeData[0]
    }

    let r = 0;
    
    switch (funInfo.funName) {


        //一元函数
        case 'func1':
            r = funInfo.func1(computeData[0])
        break;
        
        case 'boxcox_0':
            r = funInfo.boxcox_0(computeData[0]);
        break;

        case 'boxcox_1_2':
            r = funInfo.boxcox_1_2(computeData[0]);
        break;

        case 'boxcox_1':
            r = funInfo.boxcox_1(computeData[0]);
        break;

        case 'boxcox_2':
            r = funInfo.boxcox_2(computeData[0]);
        break;



        // 二元函数
        case 'func4':
            let x = computeData[0]
            let y = computeData[1]
            r = (1 - Math.sin(x)) * (Math.log(Math.abs(y)+1)) **2
        break;

        case '+':
            for (let i = 0; i < computeData.length; i++) {
                r += computeData[i]
            }
        break;

        case '-':
            r = computeData[0];
            for (let i = 1; i < computeData.length; i++) {
                r -= computeData[i]
            }
        break;

        case '*':
            r = computeData[0];
            for (let i = 1; i < computeData.length; i++) {
                r = r * computeData[i]
            }
        break;

        case '/':
            r = computeData[0];
            for (let i = 1; i < computeData.length; i++) {
                if(computeData[i] == 0){
                    r=0
                }else{
                    r /= computeData[i]
                }
            }
        break;

        case 'x^y':
            r = computeData[0] ** computeData[1];
        break;

        case '(1-x^3)(1-y^3)^2':
            r = ((1 - computeData[0] ** 3) * (1 - computeData[1] ** 3)) ** 2;
        break;

        case 'mean':
            for (let i = 0; i < computeData.length; i++) {
                r += computeData[i]
            }
            r /= computeData.length;
        break;

        case 'variance':
            let mean = 0;
            for (let i = 0; i < computeData.length; i++) {
                mean += computeData[i]
            }
            mean /= computeData.length;

            for (let i = 0; i < computeData.length; i++) {
                r += (computeData[i] - mean) ** 2
            }
            r /= computeData.length;
        break;

        case 'sin':
            r = Math.sin(computeData[0]);
        break;
        

    }

    return r;
    
}

function removeEndChar(str, charToRemove) {
    const regex = new RegExp(charToRemove + '$'); // 创建正则表达式，匹配结尾的字符
    return str.replace(regex, ''); // 替换为空字符串
}

async function Case0(table_name,columns,symbol,params,width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4){

    console.log('Case0')
    //console.log(table_name,columns,symbol,params,width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4)

     //对单点函数，extremes是极值点；对均值，extremes是区间长度；对加权均值，extremes是加权数组， 如[1,-1,3,1,-1]
     symbol = symbol.split(';')
     if (symbol.length > 1) {
         params = symbol[1].split(',')
     } else {
         params = []
     }

     let funInfo = new FunInfo(symbol[0],params)



    let table = mergeTables(table_name,columns,funInfo, 'tv')
    let results = []


    let sql = `select t `
    for(let i=0;i<columns.length;i++){
        sql = `${sql},${columns[i]} `
    }

    sql = `${sql} from ${table_name} `
    if (endTime >= 0){
        sql = sql.concat(`where t between ${startTime} and ${endTime} `);
    }
    sql = sql.concat('order by t asc')
    console.log(`sql: ${sql}\n`);


    timestart("readTableBFromDB")
    let r = await readTableBFromDB(sql)
    timeend("readTableBFromDB")

    startTime = r[0][0];
    endTime = r[r.length - 1][0];
    //console.log(r)

    timestart("compute")
    let result = []
    for(let i=0;i<r.length;i++){
        let computeData = []
        for(let j=0;j<columns.length;j++){
            //第一列为t，往后为v1、v2...
            computeData.push(r[i][j+1])
        }

        let v = compute(computeData, funInfo, interact_type)
        let pair = { t: r[i][0], v: v };

        result.push(pair)

    }
    timeend("compute")



    if(mode == 'multi'){
        let result_array = []
        for(let i=0;i<results.length;i++){
            let result =results[i]
            result_array.push(result)
        }
    
        return result_array
    }

    return result
}


// 两表分别从数据库取出来，程序做加法，程序做M4
async function Case1(table_name,columns,symbol,params,width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4){

    console.log('Case123123')

    return "success"
    //console.log(table_name,columns,symbol,params,width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4)

     //对单点函数，extremes是极值点；对均值，extremes是区间长度；对加权均值，extremes是加权数组， 如[1,-1,3,1,-1]
     symbol = symbol.split(';')
     if (symbol.length > 1) {
         params = symbol[1].split(',')
     } else {
         params = []
     }

     let funInfo = new FunInfo(symbol[0],params)



    let table = mergeTables(table_name,columns,funInfo, 'tv')
    let results = []

    


    let M4_arrays = [];
    let min_values = [];
    let max_values = [];

    let dataLength = 0

    let sql = `select t `
    for(let i=0;i<columns.length;i++){
        sql = `${sql},${columns[i]} `
    }

    sql = `${sql} from ${table_name} `
    if (endTime >= 0){
        sql = sql.concat(`where t between ${startTime} and ${endTime} `);
    }
    sql = sql.concat('order by t asc')
    console.log(`sql: ${sql}\n`);


    timestart("readTableBFromDB")
    let r = await readTableBFromDB(sql)
    timeend("readTableBFromDB")

    startTime = r[0][0];
    endTime = r[r.length - 1][0];
    //console.log(r)

    timestart("compute")
    let result = []
    for(let i=0;i<r.length;i++){
        let computeData = []
        for(let j=0;j<columns.length;j++){
            //第一列为t，往后为v1、v2...
            computeData.push(r[i][j+1])
        }

        let v = compute(computeData, funInfo, interact_type)
        let pair = { t: r[i][0], v: v };

        result.push(pair)

    }
    timeend("compute")



    if(mode == 'multi'){
        for(let i=0;i<results.length;i++){
            let result =results[i]
            let {M4_array: M4_array, min_value: min_value, max_value: max_value} = generateM4(result, width, startTime, endTime);
            M4_arrays.push(M4_array);
            min_values.push(min_value);
            max_values.push(max_value);

        }
    
        return {
            M4_array: M4_arrays,
            min_value: min_values,
            max_value: max_values
        }
    }


    timestart("generateM4")
    let {M4_array: M4_array, min_value: min_value, max_value: max_value} = generateM4(result, width, startTime, endTime);
    timeend("generateM4")

    M4_arrays.push(M4_array);
    min_values.push(min_value);
    max_values.push(max_value);

    screen_m4.M4_array = M4_array
    screen_m4.dataReductionRatio =0


    return {
        M4_array: M4_arrays,
        min_value: min_values,
        max_value: max_values
    }

}


 // 在数据库：两表在数据相加后，对结果做M4
 async function Case2(table_name,columns,symbol,params,width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4){
    //console.log('Case2')

    //console.log(table_name1,table_name_others,symbol,params,width,height,mode,parallel,errorBound,startTime,endTime, interact_type)
     //对单点函数，extremes是极值点；对均值，extremes是区间长度；对加权均值，extremes是加权数组， 如[1,-1,3,1,-1]
     symbol = symbol.split(';')
     if (symbol.length > 1) {
         params = symbol[1].split(',')
     } else {
         params = []
     }




     let funInfo = new FunInfo(symbol[0],params)
     symbol = funInfo.funName

     table_name = mergeTables(table_name,columns,funInfo, 'tv')


     let column1 = columns[0];
     let column_others = columns.slice(1);
 
 
 
      if(endTime < 0){
         let sql = `select count(*) as c from ${table_name}`
         let result1 = await pool.query(sql);
         
         let num = result1.rows[0].c
 
         endTime = num-1
      }
 
 
     let sql_query = `WITH Q AS (select t as t, `
 
     if (symbol == '+' | symbol == '-' | symbol == '*'){
         sql_query = sql_query.concat(`(${column1}`);
         column_others.forEach(order => {
             sql_query = sql_query.concat(` ${symbol} ${order}`)
         });
         sql_query = sql_query.concat(`) AS v
             from ${table_name} `);
     } else if (symbol == '/'){
         sql_query = sql_query.concat(`(COALESCE(${column1}`);
         column_others.forEach(order => {
             sql_query = sql_query.concat(` ${symbol} nullif(${order}, 0)`)
         });
         sql_query = sql_query.concat(`, 0))AS v
             from ${table_name} `);
     }else if (symbol == 'mean'){
         let weight = 1 / (column_others.length + 1);
         sql_query = sql_query.concat(`${weight} * ${column1}`);
         column_others.forEach(order => {
             sql_query = sql_query.concat(` + ${weight} * ${order}`)
         });
         sql_query = sql_query.concat(` AS v
             from ${table_name} `);
     } else if (symbol == 'variance'){
         let weight = 1 / (column_others.length + 1);
         let mean = `(${weight} * ${column1}`;
         column_others.forEach(order => {
             mean = mean.concat(` + ${weight} * ${order}`)
         });
         mean = mean.concat(`)`);
         sql_query = sql_query.concat(`${weight} * (${column1} - ${mean})^2`);
         column_others.forEach(order => {
             sql_query = sql_query.concat(` + ${weight} * (${order} - ${mean})^2`)
         });
         sql_query = sql_query.concat(` AS v
             from ${table_name} `);
     } else if (symbol == 'boxcox_0'){
         sql_query = sql_query.concat(`COALESCE(LN(nullif(${column1}, 0)), 0) as v from ${table_name}\n`);
     } else if (symbol == 'boxcox_1_2'){
         sql_query = sql_query.concat(`(sqrt(${column1})-1) * 2 as v from ${table_name}\n`);
     } else if (symbol == 'boxcox_1'){
         sql_query = sql_query.concat(`${column1}-1 as v from ${table_name}\n`);
     } else if (symbol == 'boxcox_2'){
         sql_query = sql_query.concat(`(power(${column1}, 2)-1) / 2 as v from ${table_name}\n`);
     } else if (symbol == 'func1'){
         sql_query = sql_query.concat(`0.001 * power(${column1}, 3) - 3 * ${column1} as v from ${table_name}\n`);
     } else if (symbol == 'func4'){
         sql_query = sql_query.concat(`(1-sin(${column1}))*power(LN(abs(${column_others[0]})+1), 2) as v from ${table_name} `);
     }
 
     sql_query = sql_query.concat(`where t between ${startTime} and ${endTime})\n`);
 
     let sql_getM4 = `SELECT t,v,v_min,v_max FROM Q JOIN
                 (SELECT floor(${width}*(t-${startTime})::bigint / (${endTime}-${startTime}+1)) AS k,
                        min(v) AS v_min, max(v) AS v_max,
                        min(t) AS t_min, max(t) AS t_max
                  FROM Q GROUP BY k) AS QA
             ON k = round(${width}*(t-${startTime})::bigint / (${endTime}-${startTime}+1))
                 AND (t = t_min OR t = t_max)
                     order by t asc `
     
     sql = sql_query.concat(sql_getM4);
 
     console.log(`sql: ${sql}`);

     timestart('SQL.query.time');
     result1 = await pool.query(sql);
     timeend('SQL.query.time');
 
    //  result1.rows.forEach(r => {
    //      console.log(r)
    //  })
 
    let M4_array = []
    let MIN = Infinity;
    let MAX = -Infinity;
 
    for (let i = 0; i < result1.rows.length; i += 2) {

        result1.rows[i].v_min ??= 0;
        result1.rows[i].v_max ??= 0;
        result1.rows[i].v ??= 0;
        result1.rows[i + 1].v ??= 0;

        result1.rows[i].v_min = Math.min(result1.rows[i].v_min,result1.rows[i].v_max,result1.rows[i].v,result1.rows[i+1].v)
        result1.rows[i].v_max = Math.max(result1.rows[i].v_min,result1.rows[i].v_max,result1.rows[i].v,result1.rows[i+1].v)

        let pair = {
            start_time: result1.rows[i].t, end_time: result1.rows[i + 1].t,
            min: result1.rows[i].v_min, max: result1.rows[i].v_max,
            st_v: result1.rows[i].v, et_v: result1.rows[i + 1].v
        }

        if (pair.min < MIN) { MIN = pair.min; }
        if (pair.max > MAX) { MAX = pair.max; }

        M4_array.push(pair);
    }
 
    screen_m4.M4_array = M4_array
    screen_m4.dataReductionRatio =0

    //  //console.log('case2')
    //  //  outputM4(M4_array);
    //  for(let i=0;i<M4_array.length;i++){
    //      let m4 = M4_array[i]
    //      // console.log(
    //      //     'm4:',i
    //      //     ,'sT:',m4.start_time
    //      //     , ',eT:',m4.end_time
    //      //     , ',sV:',m4.st_v
    //      //     , ',eV:',m4.et_v
    //      //     , ',min:',m4.min
    //      //     , ',max:',m4.max
    //      //  )
    //  }
     return {
         M4_array: M4_array,
         min_value: MIN,
         max_value: MAX
     }
     console.log(result1.rows)
 

}




 async function sympleInitM4(segmentTrees,M4_array,func, mode, parallel, screen_m4) {

    for(let i=0;i<M4_array.length;i++){
        let m4 = M4_array[i]
        
        
        m4.st_v = m4.stNodes[0].min
        m4.et_v = m4.etNodes[0].min


        if (M4_array[i].st_v < M4_array[i].et_v) {
            M4_array[i].min = M4_array[i].st_v
            M4_array[i].max = M4_array[i].et_v

        } else {
            M4_array[i].min = M4_array[i].et_v
            M4_array[i].max = M4_array[i].st_v
        }


        //计算inner node
        //将m4.innerNodes全部放入候选队列
        for(let j=0;j<M4_array[i].innerNodes.length;j++){
            let nodePairs = M4_array[i].innerNodes[j]

            //let {tmpmin,tmpmax}=unifiedCalulate(segmentTrees, nodePairs, func, mode, false)
            let tmpmin = nodePairs[0].min
            let tmpmax = nodePairs[0].max

            if(tmpmax > M4_array[i].max){
                M4_array[i].max = tmpmax
            }

            if(tmpmin < M4_array[i].min){
                M4_array[i].min = tmpmin
            }
        }
    }
    
}


function getMinMaxOfM4(M4_array) {
    let min_value = Infinity;
    let max_value = -Infinity;
    for (let i = 0; i < M4_array.length; i++) {
        let m4 = M4_array[i];

        min_value = Math.min(min_value, m4.st_v, m4.et_v, m4.min, m4.max);
        max_value = Math.max(max_value, m4.st_v, m4.et_v, m4.min, m4.max);
    }
    return { min_value, max_value };
}

function computeForM4(width, screenStart, screenEnd, M4_arrays, funInfo, interact_type) {
    let M4_array = computeM4TimeSE(width, [screenStart, screenEnd]);

    for (let i = 0; i < width; i++) {

        //stv
        let currentNodes = [];
        for (let j = 0; j < M4_arrays.length; j++) {
            currentNodes.push(M4_arrays[j][i].st_v);
        }
        M4_array[i].st_v = compute(currentNodes, funInfo, interact_type);

        //etv
        currentNodes = [];
        for (let j = 0; j < M4_arrays.length; j++) {
            currentNodes.push(M4_arrays[j][i].et_v);
        }
        M4_array[i].et_v = compute(currentNodes, funInfo, interact_type);

        // //min
        // currentNodes = [];
        // for (let j = 0; j < M4_arrays.length; j++) {
        //     currentNodes.push(M4_arrays[j][i].min);
        // }
        // M4_array[i].min = compute(currentNodes, funInfo, interact_type);

        // //max
        // currentNodes = [];
        // for (let j = 0; j < M4_arrays.length; j++) {
        //     currentNodes.push(M4_arrays[j][i].max);
        // }
        // M4_array[i].max = compute(currentNodes, funInfo, interact_type);

        //min max together
        currentNodes = []
        for (let j = 0; j < M4_arrays.length; j++) {
            let node = new SegmentTreeNode();
            node.min = M4_arrays[j][i].min
            node.max = M4_arrays[j][i].max
            currentNodes.push(node)
        }
        let {tmpmin,tmpmax}=unifiedCalulate(null, currentNodes, funInfo, null, false)
        M4_array[i].max=tmpmax
        M4_array[i].min=tmpmin
    }
    return M4_array;
}

 // 在数据库：两表做M4，对M4相加，OM3结构数据表
 async function Case3(table_name,columns,symbol,params,width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4){

    console.log('Case3')


    let screenStart = startTime
    let screenEnd = endTime

   //对单点函数，extremes是极值点；对均值，extremes是区间长度；对加权均值，extremes是加权数组， 如[1,-1,3,1,-1]
   symbol = symbol.split(';')
   if(symbol.length > 1){
       params = symbol[1].split(',')
   }else{
       params = []
   }

   let funInfo = new FunInfo(symbol[0],params)


   let table = mergeTables(table_name,columns,funInfo,'om3')
    
    let segmentTrees = []

    let M4_arrays = []
    for(let i=0;i<columns.length;i++){
        segmentTrees.push(await buildtree(table,columns,i, width, screenStart,screenEnd))
        let realDataRowNum = getRealDataRowNum(segmentTrees[0], segmentTrees[0])

        if(isNaN(screenStart) || screenStart < 0){
            screenStart = 0
        }
        if(isNaN(screenEnd) || screenEnd<0 || screenEnd > realDataRowNum-1){
            screenEnd = realDataRowNum-1
        }
    }

    for(let i=0;i<columns.length;i++){

        let M4_array = computeM4TimeSE(width, [screenStart, screenEnd])

        await fenlie(M4_array, screenStart,screenEnd, [segmentTrees[i]], funInfo, []);
        sympleInitM4([segmentTrees[i]],M4_array,funInfo, mode, parallel, null)
    
        M4_arrays.push(M4_array)

    }


    let M4_array = computeForM4(width, screenStart, screenEnd, M4_arrays, funInfo, interact_type);





     M4_arrays = []
    let min_values = [];
    let max_values = [];
    let { min_value, max_value } = getMinMaxOfM4(M4_array);

    //let {M4_array: M4_array, min_value: min_value, max_value: max_value} = generateM4(result, width, startTime, endTime);

    M4_arrays.push(M4_array);
    min_values.push(min_value);
    max_values.push(max_value);


    screen_m4.M4_array = M4_array
    screen_m4.dataReductionRatio = 1 - segmentTrees[0].nodeCount*2 / segmentTrees[0].realDataNum

    return {
        M4_array: M4_arrays,
        min_value: min_values,
        max_value: max_values
    }


 }

 // 在数据库：两表做M4，对M4相加，t-v结构数据表
 async function Case4(table_name,columns,symbol,params,width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4){

    console.log('Case4')


   //对单点函数，extremes是极值点；对均值，extremes是区间长度；对加权均值，extremes是加权数组， 如[1,-1,3,1,-1]
   symbol = symbol.split(';')
   if(symbol.length > 1){
       params = symbol[1].split(',')
   }else{
       params = []
   }

   let funInfo = new  FunInfo(symbol[0],params)
   symbol = funInfo.funName

   let table = mergeTables(table_name,columns,funInfo,'tv')
    if (endTime < 0) {
        let sql = `select count(*) as c from ${table}`

        timestart('SQL.query.time');
        let result1 = await pool.query(sql);
        timeend('SQL.query.time');

        let num = result1.rows[0].c

        endTime = num - 1
    }

    let screenStart = startTime
    let screenEnd = endTime

    let M4_arrays = []
    for (let i = 0; i < columns.length; i++){
        // let sql_getM4 = `SELECT t,v,v_min,v_max FROM ${table} JOIN
        // (SELECT round(${width}*(t-${startTime})::bigint / (${endTime}-${startTime}+1)) AS k,
        //        min(v) AS v_min, max(v) AS v_max,
        //        min(t) AS t_min, max(t) AS t_max
        // FROM ${table} GROUP BY k) AS QA
        // ON k = round(${width}*(t-${startTime})::bigint / (${endTime}-${startTime}+1))
        //     AND (t = t_min OR t = t_max)
        // order by t asc `


        let sql_getM4 = `SELECT t,${columns[i]} as v,v_min,v_max FROM ${table} JOIN
        (SELECT round(${width}*(t-${startTime})::bigint / (${endTime}-${startTime}+1)) AS k,
               min(${columns[i]}) AS v_min, max(${columns[i]}) AS v_max,
               min(t) AS t_min, max(t) AS t_max
        FROM ${table} WHERE t BETWEEN ${startTime} AND ${endTime} GROUP BY k) AS QA
        ON k = round(${width}*(t-${startTime})::bigint / (${endTime}-${startTime}+1))
            AND (t = t_min OR t = t_max)
        order by t asc `


        //console.log(`sql: ${sql}`);

        timestart('SQL.query.time');
        let result1 = await pool.query(sql_getM4);
        timeend('SQL.query.time');

        let M4_array = []
        for (let i = 0; i < result1.rows.length; i+=2){

             result1.rows[i].v_min ??= 0;
             result1.rows[i].v_max ??= 0;
             result1.rows[i].v ??= 0;
             result1.rows[i+1].v ??= 0;
    
             result1.rows[i].v_min = Math.min(result1.rows[i].v_min,result1.rows[i].v_max,result1.rows[i].v,result1.rows[i+1].v)
             result1.rows[i].v_max = Math.max(result1.rows[i].v_min,result1.rows[i].v_max,result1.rows[i].v,result1.rows[i+1].v)
    

             

            let pair = {start_time:result1.rows[i].t, end_time:result1.rows[i+1].t, 
                min:result1.rows[i].v_min, max:result1.rows[i].v_max, 
                st_v:result1.rows[i].v, et_v:result1.rows[i+1].v}
                
            // if (pair.min < MIN) {MIN = pair.min;}
            // if (pair.max > MAX) {MAX = pair.max;}
            
            M4_array.push(pair);
        }

        M4_arrays.push(M4_array)
    }

    let M4_array = computeForM4(width, screenStart, screenEnd, M4_arrays, funInfo, interact_type);


    M4_arrays = []
    let min_values = [];
    let max_values = [];
    let { min_value, max_value } = getMinMaxOfM4(M4_array);

    //let {M4_array: M4_array, min_value: min_value, max_value: max_value} = generateM4(result, width, startTime, endTime);

    M4_arrays.push(M4_array);
    min_values.push(min_value);
    max_values.push(max_value);


    screen_m4.M4_array = M4_array
    screen_m4.dataReductionRatio = 1 - width*4 / (endTime-startTime+1)

    return {
        M4_array: M4_arrays,
        min_value: min_values,
        max_value: max_values
    }



}


// 数据库做加法，数据库做M4 -----influxDB
async function Case5(table_name,columns,symbol,params,width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4){

    console.log('Case5')
   
    //console.log(table_name1,table_name_others,symbol,params,width,height,mode,parallel,errorBound,startTime,endTime, interact_type)
     //对单点函数，extremes是极值点；对均值，extremes是区间长度；对加权均值，extremes是加权数组， 如[1,-1,3,1,-1]
     symbol = symbol.split(';')
     if (symbol.length > 1) {
         params = symbol[1].split(',')
     } else {
         params = []
     }

     let funInfo = new FunInfo(symbol[0],params)
     symbol = funInfo.funName

     let tableName = mergeTables(table_name,columns,funInfo, 'tv')



    const influxUrl = dbConfig['influxUrl'];
    const token = dbConfig['token'];
    const org = dbConfig['org'];
    const bucket = dbConfig['bucket'];

    const { InfluxDB } = require('@influxdata/influxdb-client');
    //创建 InfluxDB 客户端实例
    const influxDB = new InfluxDB({
        url: influxUrl,
        token: token,
        timeout: 24*60*60*1000  // 增加超时时间，例如60秒
    })
    // 创建查询客户端
    const queryApi = influxDB.getQueryApi(org);


    // for test!!!!!!!!
    //tableName = 'little'

    if(endTime < 0){
        let rows = await getTime(queryApi, bucket, tableName);

        let { starttime, endtime } = timeSE(rows);

        startTime = starttime;
        endTime = endtime;
        console.log('Start Time:', startTime, 'End Time:', endTime);
     }

    try {
        // 计算窗口持续时间
        const windowDuration = calculateWindowDuration(startTime, endTime, width);
        console.log(`Calculated window duration: ${windowDuration}`);

        // 构建 Flux 查询
        const fluxQuery = buildFluxQueryUsingWindow(bucket, tableName, columns, symbol, startTime, endTime, windowDuration);
        console.log(`Constructed Flux Query:\n${fluxQuery}`);

        // 执行查询
        timestart('SQL.query.time');
        const tables = await queryApi.collectRows(fluxQuery);
        timeend('SQL.query.time');

        //console.log(`Number of Records Returned: ${tables.length}`);
        //console.log(tables)
        // 分组结果
        const groupedResults = groupResultsByYield(tables);
    

        let M4_array = []
        for (let i = 0; i < tables.length / 4; i++) {
            let pair = {
                start_time:groupedResults.t_first[i].t, 
                st_v:      groupedResults.t_first[i].computed,
                end_time:  groupedResults.t_last[i].t,
                et_v:      groupedResults.t_last[i].computed,
                min:       groupedResults.v_min[i].computed,
                max:       groupedResults.v_max[i].computed
            }

            M4_array.push(pair);

        }


        let M4_arrays = []
        let min_values = [];
        let max_values = [];
        let { min_value, max_value } = getMinMaxOfM4(M4_array);
    
        //let {M4_array: M4_array, min_value: min_value, max_value: max_value} = generateM4(result, width, startTime, endTime);
    
        M4_arrays.push(M4_array);
        min_values.push(min_value);
        max_values.push(max_value);
    
    
        screen_m4.M4_array = M4_array
        screen_m4.dataReductionRatio = 1 - width*4 / (endTime-startTime+1)
    
        return {
            M4_array: M4_arrays,
            min_value: min_values,
            max_value: max_values
        }


    } catch (error) {
        console.error('Error:', error);
    }




 }


/**
 * 构建 Flux 查询，使用动态运算表达式
 */
function buildFluxQueryUsingWindow_multi(bucket, tableName, vColumns, symbol, startTime, endTime, windowDuration) {
    let fluxQueries = [];



    vColumns.forEach(column => {
        // 生成针对当前列的聚合计算


        const v_min = `
        ${column}_v_min = from(bucket: "${bucket}")
          |> range(start: ${startTime}, stop: ${endTime})
          |> filter(fn: (r) => r._measurement == "${tableName}")  // 过滤指定的 _measurement
          |> filter(fn: (r) => r["_field"] == "${column}" )
          |> aggregateWindow(every: ${windowDuration}, fn: min, createEmpty: false)
          |> yield(name: "${column}_v_min")
        `;



        const v_max = `
        ${column}_v_max = from(bucket: "${bucket}")
          |> range(start: ${startTime}, stop: ${endTime})
          |> filter(fn: (r) => r._measurement == "${tableName}")  // 过滤指定的 _measurement
          |> filter(fn: (r) => r["_field"] == "${column}" )
          |> aggregateWindow(every: ${windowDuration}, fn: max, createEmpty: false)
          |> yield(name: "${column}_v_max")
        `;

        const v_first = `
        ${column}_v_first = from(bucket: "${bucket}")
          |> range(start: ${startTime}, stop: ${endTime})
          |> filter(fn: (r) => r._measurement == "${tableName}")  // 过滤指定的 _measurement
          |> filter(fn: (r) => r["_field"] == "${column}" )
          |> aggregateWindow(every: ${windowDuration}, fn: first, createEmpty: false)
          |> yield(name: "${column}_v_first")
        `;

        const v_last = `
        ${column}_v_last = from(bucket: "${bucket}")
          |> range(start: ${startTime}, stop: ${endTime})
          |> filter(fn: (r) => r._measurement == "${tableName}")  // 过滤指定的 _measurement
          |> filter(fn: (r) => r["_field"] == "${column}" )
          |> aggregateWindow(every: ${windowDuration}, fn: last, createEmpty: false)
          |> yield(name: "${column}_v_last")
        `;



        // 将生成的查询添加到数组中
        fluxQueries.push(v_min, v_max, v_first, v_last);
        //fluxQueries.push(v_min);
    });

    // 将所有子查询组合成最终的 Flux 查询
    const flux = fluxQueries.join('\n');

    return flux.trim();
}

function groupResultsByYield_multi(rows) {
    const grouped = {};

    rows.forEach(row => {
        const yieldName = row.result;

        // 使用正则表达式来拆分 "temperature_v_min" -> ["temperature", "v_min"]
        const match = yieldName.match(/([a-zA-Z0-9_]+)_(v_min|v_max|v_first|v_last)/);

        if (match) {
            const columnName = match[1];  // "temperature"
            const resultType = match[2];   // "v_min", "v_max", "t_first", or "t_last"

            if (!grouped[columnName]) {
                grouped[columnName] = {};
            }

            if (!grouped[columnName][resultType]) {
                grouped[columnName][resultType] = [];
            }

            // 将行数据添加到对应的分组
            grouped[columnName][resultType].push(row);
        }
    });

    return grouped;
}

// 数据库做M4 ,M4做加法，-----influxDB
async function Case6(table_name,columns,symbol,params,width,height,mode,parallel,errorBound,startTime,endTime, interact_type,screen_m4){

    console.log('Case6')
   
    //console.log(table_name1,table_name_others,symbol,params,width,height,mode,parallel,errorBound,startTime,endTime, interact_type)
     //对单点函数，extremes是极值点；对均值，extremes是区间长度；对加权均值，extremes是加权数组， 如[1,-1,3,1,-1]
     symbol = symbol.split(';')
     if (symbol.length > 1) {
         params = symbol[1].split(',')
     } else {
         params = []
     }

     let funInfo = new FunInfo(symbol[0],params)
     symbol = funInfo.funName

     let tableName = mergeTables(table_name,columns,funInfo, 'tv')


     let column1 = columns[0];
     let column_others = columns.slice(1);
  
 



    

    

    const influxUrl = dbConfig['influxUrl'];
    const token = dbConfig['token'];
    const org = dbConfig['org'];
    const bucket = dbConfig['bucket'];



    const { InfluxDB } = require('@influxdata/influxdb-client');
    //创建 InfluxDB 客户端实例
    const influxDB = new InfluxDB({
        url: influxUrl,
        token: token,
        timeout: 24*60*60*1000  // 增加超时时间，例如60秒
    })
    // 创建查询客户端
    const queryApi = influxDB.getQueryApi(org);


    // for test!!!!!!!!
    //tableName = 'little'

    if(endTime < 0){
        let rows = await getTime(queryApi, bucket, tableName);

        let { starttime, endtime } = timeSE(rows);

        startTime = starttime;
        endTime = endtime;
        console.log('Start Time:', startTime, 'End Time:', endTime);
     }


     let screenStart = new Date(startTime);
     let screenEnd = new Date(endTime);
     screenStart = Math.floor(screenStart.getTime()/1000)
     screenEnd = Math.floor(screenEnd.getTime()/1000)

    try {
        // 计算窗口持续时间
        const windowDuration = calculateWindowDuration(startTime, endTime, width);
        console.log(`Calculated window duration: ${windowDuration}`);

        // 构建 Flux 查询
        const fluxQuery = buildFluxQueryUsingWindow_multi(bucket, tableName, columns, symbol, startTime, endTime, windowDuration);
        console.log(`Constructed Flux Query:\n${fluxQuery}`);

        // 执行查询
        timestart('SQL.query.time');
        const tables = await queryApi.collectRows(fluxQuery);
        timeend('SQL.query.time');

        //console.log(`Number of Records Returned: ${tables.length}`);
        //console.log(tables)
        // 分组结果
        const groupedResults = groupResultsByYield_multi(tables);
    
        //console.log(groupedResults)

        let M4_arrays = []
        for (let i = 0; i < columns.length; i++){

            let M4_array = computeM4TimeSE(width, [screenStart, screenEnd])

            for(let j=0;j<width;j++){

                M4_array[j].st_v = groupedResults[columns[i]]['v_first'][j]["_value"]
                M4_array[j].et_v = groupedResults[columns[i]]['v_last'][j]["_value"]
                M4_array[j].min = groupedResults[columns[i]]['v_min'][j]["_value"]
                M4_array[j].max = groupedResults[columns[i]]['v_max'][j]["_value"]
            }

            M4_arrays.push(M4_array)
        }

        let M4_array = computeForM4(width, screenStart, screenEnd, M4_arrays, funInfo, interact_type);

        M4_arrays = []
        let min_values = [];
        let max_values = [];
        let { min_value, max_value } = getMinMaxOfM4(M4_array);
    
        //let {M4_array: M4_array, min_value: min_value, max_value: max_value} = generateM4(result, width, startTime, endTime);
    
        M4_arrays.push(M4_array);
        min_values.push(min_value);
        max_values.push(max_value);
    
    
        screen_m4.M4_array = M4_array
        screen_m4.dataReductionRatio = 1 - width*4 / (endTime-startTime+1)
    
        return {
            M4_array: M4_arrays,
            min_value: min_values,
            max_value: max_values
        }


    } catch (error) {
        console.error('Error:', error);
    }




 }





async function getTime(queryApi, bucket, table) {
    const query = `


min_time = from(bucket: "${bucket}")
  |> range(start: 0)  // 查询所有数据（从 1970-01-01 00:00:00 UTC 起）
  |> filter(fn: (r) => r._measurement == "${table}")  // 过滤指定的 _measurement
  |> first()
  |> rename(columns: {_time: "min_time"})  // 重命名 _time 为 min_time
  |> keep(columns: ["min_time"])  // 只保留 min_time 字段

max_time = from(bucket: "${bucket}")
  |> range(start: 0)  // 查询所有数据（从 1970-01-01 00:00:00 UTC 起）
  |> filter(fn: (r) => r._measurement == "${table}")  // 过滤指定的 _measurement
  |> last()
  |> rename(columns: {_time: "max_time"})  // 重命名 _time 为 max_time
  |> keep(columns: ["max_time"])  // 只保留 max_time 字段

min_time
  |> yield(name: "min_time")
max_time
  |> yield(name: "max_time")

    `;

    console.log(query)

    timestart('SQL.query.time');
    const rows = await queryApi.collectRows(query);
    timeend('SQL.query.time');

    //console.log(rows)
    return rows;

}

function timeSE(results) {
    let starttime = '';
    let endtime = '';
    for (let i = 0; i < results.length; i++) {
        if (results[i].result === 'max_time') {
            endtime = results[i].max_time;
        }
        if (results[i].result === 'min_time') {
            starttime = results[i].min_time;
        }
    }
    return { starttime, endtime };
}

/**
 * Calculate Window Duration
 * @param {string} startTime - Start time (ISO 8601)
 * @param {string} endTime - End time (ISO 8601)
 * @param {number} width - Number of windows
 * @returns {string} - Flux duration string
 */
function calculateWindowDuration(startTime, endTime, width) {
    const startDate = new Date(startTime).getTime();
    const endDate = new Date(endTime).getTime();
    const durationMs = endDate - startDate;
    const windowDurationMs = Math.floor(durationMs / width);

    console.log('Start Date (ms):', startDate, 'End Date (ms):', endDate, 'Duration (ms):', durationMs, 'Window Duration (ms):', windowDurationMs);

    return `${windowDurationMs}ms`;
}

function getFilters(vColumns){
    if(vColumns == null){
        return ''
    }

    let filters = ` r["_field"] == "t" `

    for(let i=0;i<vColumns.length;i++){
        filters = `${filters} or r["_field"] == "${vColumns[i]}"`
    }



    return filters 

}


/**
 * 构建 Flux 查询，使用动态运算表达式
 */
function buildFluxQueryUsingWindow(bucket, tableName, vColumns, symbol, startTime, endTime, windowDuration) {
    // 生成动态运算表达式
    const computationExpr = buildComputationExpression(vColumns, symbol);

    let filters = getFilters(vColumns)

    // 构建临时表
    const pivoted = `
    import "math"

    pivoted = from(bucket: "${bucket}")
      |> range(start: ${startTime}, stop: ${endTime})
      |> filter(fn: (r) => r["_measurement"] == "${tableName}")
      |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> map(fn: (r) => ({ r with computed: ${computationExpr} }))
    `;

    // 基于临时表进行各类聚合查询
    const v_min = `
        pivoted
        |> aggregateWindow(every: ${windowDuration}, fn: min, createEmpty: false, column: "computed")
        |> yield(name: "v_min")
        `;

    const v_max = `
        pivoted
        |> aggregateWindow(every: ${windowDuration}, fn: max, createEmpty: false, column: "computed")
        |> yield(name: "v_max")
        `;

    const t_first = `
        pivoted
        |> aggregateWindow(every: ${windowDuration}, fn: first, createEmpty: false, column: "computed")
        |> yield(name: "t_first")
        `;

    const t_last = `
        pivoted
        |> aggregateWindow(every: ${windowDuration}, fn: last, createEmpty: false, column: "computed")
        |> yield(name: "t_last")
        `;

    // 将所有子查询组合成一个 Flux 查询
    let flux = `
    ${pivoted}

    ${v_min}

    ${v_max}

    ${t_first}

    ${t_last}

    `;

    // flux = `
    //     ${pivoted}
    //     pivoted
    // `


    return flux.trim();
}

/**
 * 构建计算表达式，支持任意数量的 vColumns
 */
function buildComputationExpression(vColumns, symbol) {
    let expression = '';
    switch (symbol) {
        case '+':
        case '-':
        case '*':
        case '/':
            // 生成类似 r.v1 + r.v2 + ... 的表达式
            expression = vColumns.map(col => `r.${col}`).join(` ${symbol} `);
            break;
        case 'mean':
            // 计算平均值
            expression = `(${vColumns.map(col => `float(v: r.${col})`).join(' + ')}) / float(v:${vColumns.length})`;
            break;
        case 'variance':
            if (vColumns.length === 0) {
                throw new Error(`Symbol 'variance' 需要至少一个参数`);
            }
            // 计算方差
            const meanExpression = `(${vColumns.map(col => `float(v: r.${col})`).join(' + ')}) / float(v:${vColumns.length})`;
            expression = `(${vColumns.map(col => `(float(v: r.${col}) - (${meanExpression}))^2.0`).join(' + ')}) / float(v:${vColumns.length})`;
            break;
        case 'boxcox_0':
            if (vColumns.length !== 1) {
                throw new Error(`Symbol 'boxcox_0' 需要一个参数`);
            }
            expression = `math.log(x: float(v: r.${vColumns[0]}))`;
            break;
        case 'boxcox_1':
            if (vColumns.length !== 1) {
                throw new Error(`Symbol 'boxcox_1' 需要一个参数`);
            }
            expression = `r.${vColumns[0]} - 1.0`;
            break;
        case 'boxcox_1_2':
            if (vColumns.length !== 1) {
                throw new Error(`Symbol 'boxcox_1_2' 需要一个参数`);
            }
            expression = `(math.sqrt(x: float(v: r.${vColumns[0]})) - 1.0) * 2.0`;
            break;
        case 'boxcox_2':
            if (vColumns.length !== 1) {
                throw new Error(`Symbol 'boxcox_2' 需要一个参数`);
            }
            expression = `(math.pow(x: float(v: r.${vColumns[0]}), y: 2.0) - 1.0) / 2.0`;
            break;
        case 'func1':
            if (vColumns.length !== 1) {
                throw new Error(`Symbol 'func1' 需要一个参数`);
            }
            expression = `0.001 * math.pow(x: r.${vColumns[0]}, y: 3.0) - 3.0 * r.${vColumns[0]}`;
            break;
        case 'func4':
            if (vColumns.length !== 2) {
                throw new Error(`Symbol 'func4' 需要两个参数`);
            }
            expression = `(1.0 - math.sin(x: r.${vColumns[0]})) * math.pow(x: math.log(x: math.abs(x: r.${vColumns[1]}) + 1.0), y: 2.0)`;
            break;
        default:
            throw new Error(`Unsupported symbol: ${symbol}`);
    }

    return expression;
}


function groupResultsByYield(rows) {
    const grouped = {
        v_min: [],
        v_max: [],
        t_first: [],
        t_last: []
    };

    rows.forEach(row => {
        if (row.result === 'v_min') {
            grouped.v_min.push(row);
        } else if (row.result === 'v_max') {
            grouped.v_max.push(row);
        } else if (row.result === 't_first') {
            grouped.t_first.push(row);
        } else if (row.result === 't_last') {
            grouped.t_last.push(row);
        }
    });

    return grouped;
}









