import store, { emitter, getAvgTime, GlobalState, MultiTimeSeriesObj, ViewChangeLineChartObj, ws } from ".";
import { Commit, ActionContext, ActionHandler } from 'vuex'
import axios from "axios";
// import { constructMinMaxMissTrendTree, constructTrendTree } from '../helper/wavlet-decoder';
import { constructMinMaxMissTrendTree, constructMinMaxMissTrendTreeMulti} from '../helper/wavlet-decoder';
import { v4 as uuidv4 } from 'uuid';
import * as d3 from "d3";
import LevelDataManager from "@/model/level-data-manager";
import  NoUniformColObj  from "@/model/non-uniform-col-obj";
import { formatToRenderDataForTrend, getGlobalMinMaxInfo } from "@/helper/format-data";
import md5 from "md5"
import { arrayBufferToBase64, base64ToArrayBuffer, getLevelData, openLoading } from "@/helper/util";
import { ElButtonGroup, ElLoading } from 'element-plus'
import { drawViewChangeLineChart } from "@/application/line-interaction";
import { indexGetData, indexPutData, initIndexDB } from "@/indexdb";

async function get(state: GlobalState, url: string) {

    url = 'postgres' + url;

    //const loading = openLoading();
    const { data } = await axios.get(url);
    //loading.close();
    return data;
}

async function getBuffer(state: GlobalState, url: string) {

    url = 'postgres' + url;
    // localStorage.removeItem(url)
    try {
        const timeGetCache = new Date().getTime()
        const cacheFlag = await indexGetData(url)

        if (cacheFlag && cacheFlag !== '' && cacheFlag !== undefined && cacheFlag !== null) {
            //@ts-ignore
            const flagBuffer = base64ToArrayBuffer(cacheFlag!);
            // console.log(url, "use flag cache:", flagBuffer.byteLength);
            return flagBuffer
        }

    } catch (err) {
        console.error(err)
    }


    //const loading = openLoading();
    const { data } = await axios.get(url, { responseType: 'arraybuffer' });
    if (data) {
        indexPutData(url, arrayBufferToBase64(data));
        console.log(url, " store in indexdb")
    }
    // loading.close();
    return data;
}

const loadViewChangeQueryWSMinMaxMissDataInitData: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, payload: { startTime: number, endTime: number, width: number, height: number }) => {
    let maxLevel = 0
    const currentTable = context.state.controlParams.currentTable;
    let lineInfo: any = null
    if (context.state.controlParams.currentMode === 'Default') {
        lineInfo = context.state.defaultTableMap.get(currentTable);
    } else {
        lineInfo = context.state.customTableMap.get(currentTable);
    }

    if (lineInfo === undefined) {
        throw new Error("cannot get class info");
    }
    maxLevel = lineInfo['level'];
    const startTimeStamp = new Date(lineInfo.start_time).getTime();
    let endTimeStamp = 0
    if (lineInfo.end_time !== '') {
        endTimeStamp = new Date(lineInfo.end_time).getTime();
    }
    let timeInterval = 0;
    if (lineInfo.interval !== 0) {
        timeInterval = lineInfo.interval;
    }
    //@ts-ignore
    let mode = "single";
    let width = 600;
    let type = null;
    // const combinedUrl = `/line_chart/getDataForSingleLine?mode=${mode}&width=${width}&table_name=${currentTable}&startTime=${-1}&endTime=${-1}&nteract_type=${type}`;
    const combinedUrl = `/line_chart/getDataForSingleLine?mode=${mode}&width=${width}&table_name=${currentTable}&startTime=${-1}&endTime=${-1}&nteract_type=${type}`;
    const data = get(context.state, combinedUrl);
    data.then(tempRes => {
        const viewChangeQueryObj: ViewChangeLineChartObj = {
            id: uuidv4(),
            width: payload.width,
            height: payload.height,
            x: Math.random() * 60,
            y: Math.random() * 60,
            // root: trendTree,
            // data: { powRenderData: [], noPowRenderData: [], minv: minv!, maxv: maxv! },
            // timeRange: [0, lineInfo['max_len']],
            // startTime: startTimeStamp,
            // endTime: endTimeStamp,
            timeRange: [0, 65536],
            startTime: 0,
            endTime: 65536,
            // algorithm: "",
            // dataManager: null,
            // params: [0, 0],
            minV: 0,
            maxV: 0,
            currentLevel: Math.ceil(Math.log2(payload.width)),
            isPow: false,
            nonUniformColObjs: [],
            // maxLen: lineInfo['max_len']
            maxLen: 65536,
            dataMaxLen: 0
        }
        const drawer = drawViewChangeLineChart(viewChangeQueryObj, null)
        drawer(tempRes);
    });
}

const loadMultiTimeSeriesInitData: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, payload: { width: number, height: number, type: string }) => {
    const currentLevel = Math.ceil(Math.log2(payload.width));
    let maxLevel = 0;
    let realDataRowNum = 65536;
    const currentMulitLineClass = context.state.controlParams.currentMultiLineClass;
    let lineClassInfo: any = null
    if (context.state.controlParams.currentMode === 'Default') {
        lineClassInfo = context.state.allMultiLineClassInfoMap.get(currentMulitLineClass);
    } else {
        lineClassInfo = context.state.allCustomMultiLineClassInfoMap.get(currentMulitLineClass);
    }

    if (lineClassInfo === undefined) {
        throw new Error("cannot get class info");
    }
    maxLevel = lineClassInfo['level'];

    const combinedUrl = `/line_chart/getDataForMultiLines?width=${2 ** currentLevel}&class_name=${currentMulitLineClass}&mode=${context.state.controlParams.currentMode}`;
    const data = get(context.state, combinedUrl);

    data.then(res => {
        const startTimeStamp = new Date(lineClassInfo.start_time).getTime();
        let endTimeStamp = 0
        if (lineClassInfo.end_time !== '') {
            endTimeStamp = new Date(lineClassInfo.end_time).getTime();
        }
        let timeInterval = 0;
        if (lineClassInfo.interval !== 0) {
            timeInterval = lineClassInfo.interval;
        }
        context.commit("addMultiTimeSeriesObj", {
            className: lineClassInfo.name,
            lineAmount: lineClassInfo.amount,
            startTimeStamp: startTimeStamp,
            endTimeStamp: endTimeStamp,
            timeIntervalMs: timeInterval,                
            columnInfos: res, 
            startTime: 0, 
            endTime: realDataRowNum - 1, 
            algorithm: "multitimeseries", 
            width: payload.width, 
            height: payload.height, 
            pow: false, 
            minv: 0, 
            maxv: 0, 
            maxLevel
        })
    });
}


const Experiment: ActionHandler<GlobalState, GlobalState> = async (context: ActionContext<GlobalState, GlobalState>, params:any) =>{

    console.log("Experiment Start......")

    let functions= ["func1","boxcox_0","boxcox_1_2","boxcox_1","boxcox_2","+", "-", "*", "/", "func4", "mean", "variance"]
    let experiments=["ours","case1","case2","case3","case4","case5", "case6", "case7"]
    let dataset = ["stockdata","traffic","sensordata","nycdata","inteldata","soccerdata","synthetic_1m","synthetic_2m","synthetic_4m","synthetic_8m","synthetic_16m","synthetic_32m","synthetic_64m","synthetic_128m","synthetic_256m"]
    let widths=[200, 400, 600, 800, 1000, 1200]

    let count = 0;
    let sumNetTime = 0;
    let sumDrawTime = 0

    for(let i=0;i<widths.length;i++){
        for(let j=0;j<dataset.length;j++){
            //for(let k=0;k<functions.length;k++){
            let symbol='plus'
            let experiment = 'case6'

            let width=widths[i]
            let table_name = dataset[j]

            let columns = ['v1', 'v2']
            let height=600;
            let mode = 'compute'
            let parallel = 1;
            let errorBound = 0.05
            let startTime = 0;
            let endTime = -1;
            let interact_type = ''
            let combinedUrl = `/line_chart/start?table_name=${table_name}&columns=${columns}&symbol=${symbol}&mode=${mode}&width=${width}&height=${height}&startTime=${startTime}&endTime=${endTime}&interact_type=${interact_type}&experiment=${experiment}&parallel=${parallel}&errorBound=${errorBound}`;
            
            let start_getserver = performance.now()/1000
            const data = await get(context.state, combinedUrl);
            let end_getserver = performance.now()/1000

            if (mode == 'compute') {
                const viewChangeQueryObj: ViewChangeLineChartObj = {
                    id: uuidv4(),
                    width: width,
                    height: height,
                    x: Math.random() * 60,
                    y: Math.random() * 60,

                    timeRange: [startTime, endTime],
                    startTime: startTime,
                    endTime: endTime,
                    minV: data['min_values'][0],
                    maxV: data['max_values'][0],
                    currentLevel: Math.ceil(Math.log2(width)),
                    isPow: false,
                    nonUniformColObjs: [],
                    // maxLen: lineInfo['max_len']
                    maxLen: data['M4_arrays'][0][data['M4_arrays'][0].length - 1].et,
                    dataMaxLen: 0
                }

                let start_draw = performance.now() / 1000
                const drawer = drawViewChangeLineChart(viewChangeQueryObj, params)
                await drawer(data['M4_arrays']);
                let end_draw = performance.now() / 1000

                let server_time = end_getserver - start_getserver
                let draw_time = end_draw - start_draw
                let totaltime = end_draw - start_getserver
                let computetime = data['totaltime']
                console.log('computetime:',data['totaltime'])
                let nettime = server_time-computetime

                count ++;
                sumNetTime += nettime
                sumDrawTime += draw_time

                console.log(width, table_name, 'totaltime:', totaltime, 'server_time:', server_time,'nettime:',nettime, 'draw_time:', draw_time)


            }
            else {
                console.log('error...')
            }


                // data.then(tempRes => {
                    
                // });





            //}
        }
    }

    console.log('count:',count,'avgNetTime:',sumNetTime/count,'avgDrawTime:',sumDrawTime/count)
    console.log("Experiment End......")


   
}


let meanArray:any;
let min:number;
let max:number;
const computeLineTransform: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, params:any) =>{

    let table_name = params[0];
    let columns = params[1];
    let symbol = params[2];
    let experiment = params[3];
    let width = params[4].width
    let height = params[4].height
    let mode = params[6]
    let parallel = 1;
    let errorBound = params[5]._value;
    let startTime = params[8];
    let endTime = params[9];
    let interact_type = ''
    let aggregate = params[7]

    if(symbol == '+'){
        symbol='plus'
    }else if(symbol == '-'){
        symbol='minus'
    }else if(symbol == '*'){
        symbol='multi'
    }else if(symbol == '/'){
        symbol='devide'
    }


    console.log(table_name,experiment,columns,symbol,'',width,height,mode,parallel,errorBound,startTime,endTime, interact_type)

//http://10.16.13.21:35811/postgres/line_chart/start?table_name=testto&columns=v1,v2,v3,v4&symbol=+&mode=single&width=600&height=600&startTime=0&endTime=-1&interact_type=zoom&experiment=ours&parallel=0&errorBound=0.01
    let combinedUrl 
    // if(symbol == '')
        combinedUrl= `/line_chart/start?table_name=${table_name}&columns=${columns}&symbol=${symbol}&mode=${mode}&width=${width}&height=${height}&startTime=${startTime}&endTime=${endTime}&interact_type=${interact_type}&experiment=${experiment}&parallel=${parallel}&errorBound=${errorBound}&aggregate=${aggregate}`;
    // else
    //     combinedUrl= `/line_chart/start_progressive?table_name=${table_name}&columns=${columns}&symbol=${symbol}&mode=${mode}&width=${width}&height=${height}&startTime=${startTime}&endTime=${endTime}&interact_type=${interact_type}&experiment=${experiment}&parallel=${parallel}&errorBound=${errorBound}&aggregate=${aggregate}`;
    
    // store.state.controlParams.startTime = 0;
    // store.state.controlParams.endTime = -1;

    const data = get(context.state, combinedUrl);
    console.log('start:',data)
    const realDataRowNum = 65536; 
    const payload = params[4];
    const currentLevel = Math.ceil(Math.log2(payload.width));
    let maxLevel = 0
    let type = '';

    data.then(tempRes => {
        console.log(tempRes);
        if(symbol != ''){
            let startTimeStamp = tempRes['M4_arrays'][0][0].timestamp;
            let endTimeStamp = tempRes['M4_arrays'][0][tempRes['M4_arrays'][0].length-1].timestamp;
            const viewChangeQueryObj: ViewChangeLineChartObj = {
                id: uuidv4(),
                width: payload.width,
                height: payload.height,
                x: Math.random() * 60,
                y: Math.random() * 60,

                timeRange: [startTime, tempRes['M4_arrays'][0][tempRes['M4_arrays'][0].length-1].et],
                startTime: startTimeStamp*1000,
                endTime: endTimeStamp*1000,
                // algorithm: "",
                // dataManager: null,
                // params: [0, 0],
                minV: tempRes['min_values'][0],
                maxV: tempRes['max_values'][0],
                currentLevel: Math.ceil(Math.log2(payload.width)),
                isPow: false,
                nonUniformColObjs: [],
                // maxLen: lineInfo['max_len']
                dataMaxLen: tempRes['dataMaxLen'],
                maxLen: tempRes['M4_arrays'][0][tempRes['M4_arrays'][0].length-1].et,
            }
            // params.push(tempRes['colomns']);
            const drawer = drawViewChangeLineChart(viewChangeQueryObj, params)
            drawer(tempRes['M4_arrays']);
        }
        else{
            const startTimeStamp = tempRes['M4_arrays'][0][0].timestamp;
            let endTimeStamp = tempRes['M4_arrays'][0][tempRes['M4_arrays'][0].length-1].timestamp
            let timeInterval = 0;

            // if(symbol != 'mean'){
            //     meanArray = [...tempRes['M4_arrays']];
            //     min = tempRes['min_values'][0];
            //     max = tempRes['max_values'][0];
            // }
            // else if(symbol == 'mean'){
            //     tempRes['M4_arrays'].unshift(...meanArray);
            //     tempRes['min_values'][0] = min;
            //     tempRes['max_values'][0] = max;
            // }
            params.push(tempRes['columns'])
            context.commit("addMultiTimeSeriesObj", {
                // className: lineClassInfo.name,
                // lineAmount: lineClassInfo.amount,
                className: "lineClassInfo.name",
                lineAmount: 0,
                startTimeStamp: startTimeStamp*1000,
                endTimeStamp: endTimeStamp*1000,
                timeIntervalMs: timeInterval,                
                columnInfos: tempRes['M4_arrays'], 
                startTime: startTime, 
                endTime: tempRes['M4_arrays'][0][tempRes['M4_arrays'][0].length-1].et, 
                algorithm: "multitimeseries", 
                width: payload.width, 
                height: payload.height, 
                pow: false, 
                minv: tempRes['min_values'][0],
                maxv: tempRes['max_values'][0],
                maxLevel,
                columnsColor: columns,
                line1: params
            })
        }
    });
}

const getAllTables: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    const combinedUrl = `/line_chart/getAllTables`;
    const data = get(context.state, combinedUrl);
    data.then(res => {
        const allTables = res.map((v: any) => v['table_fullname'].split(".")[1]);
        context.commit("updateAllTables", { tables: allTables });
    })
}

async function getAllFlagsFunc(context: ActionContext<GlobalState, GlobalState>, lineType: string, isLoading: boolean) {

    const combinedUrl1 = `/line_chart/getAllFlagNames?line_type=${lineType}`;
    const allFlagNames = await get(context.state, combinedUrl1);
    const flagMap: any = {}

    let loading = null;
    if (isLoading) {
        loading = openLoading("Loading Order Coefficients. First load may take a long time, Please Wait!")
    }
    for (let i = 0; i < allFlagNames['data'].length; i++) {
        const combinedUrl2 = `/line_chart/getSingleFlag?name=${allFlagNames['data'][i]}&line_type=${lineType}`
        const tempFlagInfo = await getBuffer(context.state, combinedUrl2);
        //@ts-ignore
        flagMap[allFlagNames['data'][i].split(".")[0]] = Buffer.from(tempFlagInfo)
    }
    if (loading) {
        loading.close()
    }

    return flagMap;
}

const getAllFlags: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    initIndexDB().then(() => {
        console.log("indexdb init success");
    }).catch(() => {
        console.error("indexdb init error");
    })
    const lineType = store.state.controlParams.currentLineType
    getAllFlagsFunc(context, lineType, true).then(res => {
        context.commit("updateAllFlags", { flags: res });
        getAllFlagsFunc(context, "Multi", false).then(res => {
            context.commit("updateAllFlags", { flags: res });
        })
    })


}

const getAllMultiLineClassInfo: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    const combinedUrl = `/line_chart/getAllMultiLineClassInfo?mode=${store.state.controlParams.currentMode}`;
    const data = get(context.state, combinedUrl);
    data.then(res => {
        // console.log("getAllMultiLineClassInfo", res);
        context.commit("updateMultiLineClassInfo", { info: res });
    });
}

const getAllMultiLineClassAndLinesInfo: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    const combinedUrl = `/line_chart/getAllMultiLineClassAndLinesInfo?mode=${store.state.controlParams.currentMode}`;
    const data = get(context.state, combinedUrl);
    data.then(res => {
        // console.log("getAllMultiLineClassAndLinesInfo", res);
        context.commit("updateMultiLineClassAndLinesInfo", { info: res });
    });
}


const gettables: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    const combinedUrl = `/line_chart/gettables`;
    const data = get(context.state, combinedUrl);
    data.then(res => {
        //console.log("gettables", res);
        context.commit("gettables", { info: res });
    });

    
}

const getcolumns: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    const combinedUrl = `/line_chart/getcolumns?table_name=${store.state.controlParams.currentMultiLineClass}`;
    const data = get(context.state, combinedUrl);
    data.then(res => {
        //console.log("getcolumns", res);
        context.commit("getcolumns", { info: res });
    });
}

const getaggregates: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    const combinedUrl = `/line_chart/getaggregates?table_name=${store.state.controlParams.currentMultiLineClass}`;
    const data = get(context.state, combinedUrl);
    data.then(res => {
        context.commit("getaggregates", { info: res });
    });
}

const getfunctions: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    const combinedUrl = `/line_chart/getfunctions?table_name=${store.state.controlParams.currentMultiLineClass}`;
    const data = get(context.state, combinedUrl);
    data.then(res => {
        context.commit("getfunctions", { info: res });
    });
}

const getexperiment: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    const combinedUrl = `/line_chart/getexperiment?table_name=${store.state.controlParams.currentMultiLineClass}`;
    const data = get(context.state, combinedUrl);
    data.then(res => {
        context.commit("getexperiment", { info: res });
    });
}

const testCustomDBConn: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, payload: { hostName: string, possword: string, dbName: string, userName: string }) => {
    return axios.post("postgres/line_chart/testDBConnection", {
        host_name: payload.hostName,
        user_name: payload.userName,
        password: payload.possword,
        db_name: payload.dbName,
    })
}

const createCustomDBConn: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, payload: { hostName: string, possword: string, dbName: string, userName: string }) => {
    return axios.post("postgres/line_chart/createCustomDBConn", {
        host_name: payload.hostName,
        user_name: payload.userName,
        password: payload.possword,
        db_name: payload.dbName,
    })
}
const initOM3DB: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, payload: { hostName: string, possword: string, dbName: string, userName: string }) => {
    return get(context.state, "/line_chart/initOM3DBEnv")
}
const clearOM3Table: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, payload: { hostName: string, possword: string, dbName: string, userName: string }) => {
    return get(context.state, "/line_chart/clearOM3Table")
}

const getAllCustomTables: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    const combinedUrl = `/line_chart/getAllCustomTables`;
    return get(context.state, combinedUrl);
}

const performTransformForSingeLine: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, payload: { startTime: string, endTime: string, tableName: string }) => {
    const combinedUrl = `/line_chart/performTransformForSingeLine?start_time=${payload.startTime}&end_time=${payload.endTime}&table_name=${payload.tableName}`;
    return get(context.state, combinedUrl);
}
const performTransformForMultiLine: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, payload: { startTime: string, endTime: string, tableNames: Array<string>, multiLineClassName: string }) => {
    const combinedUrl = `/line_chart/performTransformForMultiLine?start_time=${payload.startTime}&end_time=${payload.endTime}&table_name=${payload.tableNames}&line_class=${payload.multiLineClassName}`;
    return get(context.state, combinedUrl);
}

const loadCustomTableAndInfo: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    const combinedUrl = `/line_chart/getAllCustomTableAndInfo?mode=${context.state.controlParams.currentMode}`;
    get(context.state, combinedUrl).then((res) => {
        if (res['code'] === 200) {
            context.commit("updateCustomTableAndInfo", { customTables: res['data']['table_name'], customTableInfo: res['data']['table_info'] })
        } else {
            console.log(res['msg'])
        }
    })
}

const loadDefaultTableAndInfo: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    const combinedUrl = `/line_chart/getAllDefaultTableAndInfo?mode=${context.state.controlParams.currentMode}`;
    get(context.state, combinedUrl).then((res) => {
        if (res['code'] === 200) {
            context.commit("updateDefaultTableAndInfo", { tables: res['data']['table_name'], tableInfo: res['data']['table_info'] })
        } else {

            console.log(res['msg'])
        }
    })
}



export {
    getAllTables,
    getAllCustomTables,
    getAllFlags,
    loadMultiTimeSeriesInitData,
    loadViewChangeQueryWSMinMaxMissDataInitData,//final method
    getAllMultiLineClassInfo,
    getAllMultiLineClassAndLinesInfo,
    
    gettables,
    getcolumns,
    getfunctions,
    getaggregates,
    getexperiment,

    testCustomDBConn,
    createCustomDBConn,
    initOM3DB,
    clearOM3Table,
    performTransformForSingeLine,
    loadCustomTableAndInfo,
    performTransformForMultiLine,
    loadDefaultTableAndInfo,
    computeLineTransform,
    Experiment,
}



