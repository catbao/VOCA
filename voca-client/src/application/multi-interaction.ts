import { formatNonPowDataForViewChange } from "@/helper/format-data";
import  NoUniformColObj  from "@/model/non-uniform-col-obj";
import store, { MultiTimeSeriesObj } from "@/store";
import * as d3 from 'd3';
import axios from "axios";
// import { batchViewChange, batchGetData } from "../batch/m5batch";

let nameMap: any = [
    //green
    {
        "marutisktr": "汽车-#0e7735"
    },
    {
        "tatamotorssktr": "汽车-#329a51"
    },
    {
        "eichermotsktr": "汽车-#60ba6c"
    },
    {
        "heromotocosktr": "摩托-#94d391"
    },
    {
        "mmsktr": "汽车-#c0e6ba"
    },
    //oranges
    // {
    //     "bajfinancesktr": "金融-#b93d02"
    // },
    {
        "axisbanksktr": "金融-#d14904"
    },
    {
        "indusindbksktr": "金融-#e4580b"
    },
    // {
    //     "bajajfinsvsktr": "金融-#f87f2c"
    // },
    {
        "kotakbanksktr": "金融-#fda55e"
    },


    //color
    {
        "sbinsktr": "银行-#353535"
    },
    {
        "icicibanksktr": "银行-#4d4d4d"
    },
    {
        "hdfcsktr": "银行-#626262"
    },
    {
        "hdfcbanksktr": "银行-#757575"
    },
    //blue
    {
        "infysktr": "信息-#125ca4"
    },
    {
        "techmsktr": "信息-#3181bd"
    },
    {
        "hcltechsktr": "信息-#5ba3cf"
    },
    {
        "tcssktr": "信息-#8fc1de"
    },
    //purples
    {
        "coalindiasktr": "煤炭-#5c3696"
    },
    {
        "ongcsktr": "石油气-#684ea2"
    },
    {
        "iocsktr": "石油气-#7566ae"
    },
    {
        "bpclsktr": "石油-#827cb9"
    },
    {
        "powergridsktr": "电网-#928ec3"
    },
    {
        "ntpcsktr": "光伏-#a3a0cc"
    },

    //red
    {
        "britanniasktr": "食品-#b21218"
    },
    {
        "nestleindsktr": "雀巢-#dc2a25"
    },
    {
        "tataconsumsktr": "饮料-#f6573f"
    },
    //teals 
    {
        "drreddysktr": "制药-#127273"
    },
    {
        "sunpharmasktr": "制药-#2b8b8c"
    },
    {
        "divislabsktr": "制药-#4da5a4"
    },
    //greys
    {
        "jswsteelsktr": "钢铁-#353535"
    },
    {
        "tatasteelsktr": "钢铁-#4d4d4d"
    },
    {
        "ltsktr": "工程建筑-#626262"
    },
    {
        "uplsktr": "农药-#757575"
    },
    {
        "reliancesktr": "信实工业-#888888"
    },
    {
        "grasimsktr": "纺织-#9d9d9d"
    },
    //warmgreys
    {
        "ultracemcosktr": "水泥-#665c5a"
    },
    {
        "shreecemsktr": "水泥-#7e7673"
    },
    {
        "asianpaintsktr": "涂料-#98908c"
    },
    {
        "hindalcosktr": "铝铜-#b3aaa7"
    },


    {
        "itcsktr": "贸易-#d8b5a5"
    },
    {
        "adaniportssktr": "港口运营-#fcbfd2"
    },
    {
        "titansktr": "珠宝-#ffbf79"
    },
    {
        "hindunilvrsktr": "联合利华-#d9d9d9"
    },



]
let namedMap = new Map<any, any>()
for (let i = 0; i < nameMap.length; i++) {
    const key = Object.keys(nameMap[i])[0];
    namedMap.set(key, nameMap[i][key])
}

async function get(url: string) {
    url = 'postgres' + url;
    //const loading = openLoading();
    const { data } = await axios.get(url);
    //loading.close();
    return data;
}

class InteractionInfo {
    type: string
    showInfo: Array<boolean>
    timeRange: Array<number>
    width: number
    level: number
    constructor(type: string) {
        this.type = type;
        this.showInfo = [];
        this.timeRange = [];
        this.width = 0;
        this.level = 0;
    }
    setShowInfo(showInfo: Array<boolean>) {
        this.showInfo = showInfo;
    }
    setRangeW(timeRange: Array<number>, width: number, level: number) {
        this.timeRange = [timeRange[0], timeRange[1]];
        this.width = width;
        this.level = level;
    }

}

let interactionStack: Array<InteractionInfo> = [];

export function drawMultiTimeSeries(multiTimeSeriesObj: MultiTimeSeriesObj) {

    const svgElement = document.getElementById('my_svg'); 
    if(svgElement){ 
        svgElement.remove(); 
    }

    let realTimeStampRange: Array<number> = [multiTimeSeriesObj.startTimeStamp, multiTimeSeriesObj.endTimeStamp];
    console.log("realTimeStamp:", realTimeStampRange);
    let nodeIndexRange: Array<number> = [multiTimeSeriesObj.timeRange[0], multiTimeSeriesObj.timeRange[1]]
    let rowNumber = multiTimeSeriesObj.columnInfos[0][multiTimeSeriesObj.columnInfos[0].length-1].et;
    let isInit = false;
    let isResizing = false;
    let isRebacking = false;
    let interactiveInfo = {
        startX: 0,
        startY: 0,
        offsetX: 0,
        offsetY: 0,
        isMouseDown: false,
        isMove: false,
    };
    //const pading = { top: 20, bottom: 80, left: 45, right: 250 };
    const pading = { top: 20, bottom: 80, left: 70, right: 20 };
    const svg = d3.select("#content-container").append("svg");
    svg
        .attr("width", multiTimeSeriesObj.width + pading.left + pading.right)
        .attr("height", multiTimeSeriesObj.height + pading.top + pading.bottom)
        .attr("transform", `translate(${20},${20})`)
        .style("background-color", "#fff")
        .attr("id","my_svg"); 

    const foreignId = `foreign${multiTimeSeriesObj.width + Math.random()}`;
    const foreigG = svg.append("g").attr("transfrom", `translate(${pading.left},${pading.top})`)
    let foreignObj: any = foreigG.append("foreignObject").attr("id", foreignId).attr("x", pading.left).attr("y", pading.top).attr('width', multiTimeSeriesObj.width).attr('height', multiTimeSeriesObj.height);
    const canvas = document.createElement("canvas");
    (canvas as any).__data__ = {}
    document.getElementById(foreignId)?.appendChild(canvas);
    canvas.width = multiTimeSeriesObj.width;
    canvas.height = multiTimeSeriesObj.height;
    let ctx = canvas.getContext("2d");

    // const colorArray1 = ["#b3de69", "#f2f122", "#2403db", "#ffbb78", "#f608e8", "#00ee21", "#8dd3c7", "#170b11", "#0ae9fe" , "#f2f122", "#ec030f", "#07ffed", "#2ca02c", "#98df8a", "#d62728", "#ff9896", "#9467bd", "#c5b0d5", "#8c564b", "#c49c94", "#e377c2", "#f7b6d2", "#7f7f7f", "#c7c7c7", "#bcbd22", "#dbdb8d", "#17becf", "#9edae5", "#393b79", "#5254a3", "#6b6ecf", "#9c9ede", "#637939", "#8ca252", "#b5cf6b", "#cedb9c", "#8c6d31", "#bd9e39", "#e7ba52", "#843c39", "#ad494a", "#d6616b", "#e7969c", "#7b4173", "#a55194", "#ce6dbd", "#de9ed6"];
    // const keys = colorArray1.map((_, index) => `v${index + 1}`);
    // // 生成 Map 对象
    // const colorMap = new Map(keys.map((key, index) => [key, colorArray1[index]]));
    // console.log(colorMap);


//     V7-NYC_2015-2023_Bronx_green_minute.csv：#F28E2B
    // V9-NYC_2015-2023_Bronx_yellow_minute.csv：#FFBE7D
    // V1-NYC_2015-2023_Brooklyn_green_minute.csv：#B07AA1
    // V5- NYC_2015-2023_Brooklyn_yellow_minute.csv：#D4A6C8
    // V4- NYC_2015-2023_Manhattan_green_minute.csv：#4E79A7
    // V10- NYC_2015-2023_Manhattan_yellow_minute.csv：#A0CBE8
    // V11- NYC_2015-2023_Queens_green_minute.csv：#E15759
    // V3-NYC_2015-2023_Queens_yellow_minute.csv：#FF9D9A
    // V2-'NYC_2015-2023_Staten Island_green_minute.csv'：#59A14F
    // V6-'NYC_2015-2023_Staten Island_yellow_minute.csv'：#8CD17D
    
    const colorArray1 = ["#B07AA1", "#59A14F", "#FF9D9A", "#4E79A7", "#D4A6C8", "#8CD17D", "#F28E2B", "#FFBE7D", "#A0CBE8", "#E15759"];
    // const colorArray1 = ["#b3de69", "#f2f122", "#2403db", "#ffbb78", "#f608e8", "#00ee21", "#8dd3c7", "#0ae9fe", "#170b11", "#FF0000"]
    const colorNames = [
        "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v9", "v10", "v11"
    ];
    const colorMap = new Map();
    colorNames.forEach((name, index) => {
    colorMap.set(name, colorArray1[index]);
    });

    //const timeRangeScale = d3.scaleLinear().domain([1493733884409, 1493829248294]).range([0, 2 ** 20 - 1]);
    const indexToTimeStampScale = d3.scaleLinear().domain([nodeIndexRange[0], nodeIndexRange[1]]).range([realTimeStampRange[0], realTimeStampRange[1]]);
    // const xScale: any = d3.scaleLinear().domain([0, multiTimeSeriesObj.width]).range([0, multiTimeSeriesObj.width]);
    const xScale: any = d3.scaleTime().domain([0, multiTimeSeriesObj.width]).range([0, multiTimeSeriesObj.width]);
    let showTimeXScale: any = d3.scaleTime().domain([new Date(realTimeStampRange[0]), new Date(realTimeStampRange[1])]).range([0, multiTimeSeriesObj.width]);
    // let showTimeXScale: any = d3.scaleLinear().domain([0, rowNumber]).range([0, multiTimeSeriesObj.width]);
    let yScale: any = d3.scaleLinear().domain([multiTimeSeriesObj.minv, multiTimeSeriesObj.maxv]).range([multiTimeSeriesObj.height, 0]);

    // let xReScale = d3.scaleLinear().domain([0, multiTimeSeriesObj.width]).range([0, multiTimeSeriesObj.dataManagers[0].realDataRowNum - 1]);
    let xReScale = d3.scaleLinear().domain([0, multiTimeSeriesObj.width]).range([0, rowNumber]);
    let showXTimeScale: any = d3.scaleTime().domain([new Date(realTimeStampRange[0]), new Date(realTimeStampRange[1])]).range([0, multiTimeSeriesObj.width]);
    // let showXTimeScale: any = d3.scaleLinear().domain([0, rowNumber]).range([0, multiTimeSeriesObj.width]);

    let zoomAxis = d3.axisBottom(showTimeXScale);
    let yAxis = d3.axisLeft(yScale);
    // if (store.state.controlParams.currentMode === 'Default') {
    //     yAxis = d3.axisLeft(yScale).tickFormat((val) => {
    //         //@ts-ignore
    //         return 100 * val + "%"
    //     })
    // }

    let xAxis = d3.axisBottom(showXTimeScale)
    const yReScale = d3.scaleLinear().domain([multiTimeSeriesObj.minv, multiTimeSeriesObj.maxv]).range([multiTimeSeriesObj.height, 0]);
    const timeBrushObj = d3.brushX().extent([[0, 10], [multiTimeSeriesObj.width, 40]]);
    const timeBoxBrushObj = d3.brush().extent([[pading.left, pading.top], [multiTimeSeriesObj.width + pading.left, multiTimeSeriesObj.height + pading.top]]);

    timeBoxBrushObj.on("end", timeBoxBrushed);

    timeBrushObj.on("end", brushed);
    timeBrushObj.on("start", () => {
        console.log("start")
    })
    const timeBoxG = svg.append("g").attr("transform", `translate(${pading.left},${pading.top + 50 + multiTimeSeriesObj.height - 20})`).call(timeBrushObj).call(timeBrushObj.move, [0, multiTimeSeriesObj.width]);
    // foreigG.call(timeBoxBrushObj)

    let zoomAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${multiTimeSeriesObj.height + pading.top + 50})`).attr("class", 'x axis').call(zoomAxis)
    let xAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${multiTimeSeriesObj.height + pading.top})`).attr("class", 'x axis').call(xAxis)
    let yAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${pading.top})`).attr("class", 'y axis').call(yAxis);


    //@ts-ignore
    function updateCanvasWidth() {
        //@ts-ignore
        canvas.style.width = multiTimeSeriesObj.width;
        svg
            .attr("width", multiTimeSeriesObj.width + pading.left + pading.right)
            .attr("height", multiTimeSeriesObj.height + pading.top + pading.bottom)
        foreignObj.attr("width", multiTimeSeriesObj.width);
        xScale.domain([0, multiTimeSeriesObj.width]).range([0, multiTimeSeriesObj.width]);
        showTimeXScale.domain([new Date(realTimeStampRange[0]), new Date(realTimeStampRange[1])]).range([0, multiTimeSeriesObj.width]);
        // showTimeXScale.domain([0, rowNumber]).range([0, multiTimeSeriesObj.width]);
        if (zoomAxisG != null) {
            zoomAxisG.remove();
            zoomAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${multiTimeSeriesObj.height + pading.top + 50})`).attr("class", 'x axis').call(zoomAxis)
        }
        timeBrushObj.extent([[0, 10], [multiTimeSeriesObj.width, 40]]);
        timeBrushObj.on("end", brushed);
        timeBrushObj.on("start", () => {
            console.log("start")
        })
        const tempReScale = d3.scaleLinear().domain([0, nodeIndexRange[1]]).range([0, multiTimeSeriesObj.width]);
        timeBoxG.call(timeBrushObj).call(timeBrushObj.move, [tempReScale(multiTimeSeriesObj.timeRange[0]), tempReScale(multiTimeSeriesObj.timeRange[1])]);
        ctx = canvas.getContext("2d");
    }

    let lengendG: any = null;
    
    function drawLengend(leftOffset: number, multiTimeSeriesObj: MultiTimeSeriesObj, colorArray: Array<string>) {
        if (lengendG !== null) {
            lengendG.remove()
        }
        lengendG = svg.append('g').attr("width", 100).attr("height", 700).attr("transform", `translate(${leftOffset},${pading.top - 15})`);

        // const keys = ['v1','v2','v3'];
        const keys = multiTimeSeriesObj.columnsColor;
        keys.forEach((key, index) => {
            const color = colorMap.get(multiTimeSeriesObj.columnsColor[index]);
            const label = key; 

            lengendG
                .append("rect")
                .attr("x", 0)
                .attr("y", index * 25) // 每个图例项垂直间隔 25 像素
                .attr("width", 20)
                .attr("height", 20)
                .attr("fill", color);

            lengendG
                .append("text")
                .attr("x", 30) // 文本相对于颜色块的偏移
                .attr("y", index * 25 + 15) // 与颜色块对齐并垂直居中
                .attr("font-size", "12px")
                .attr("fill", "#000")
                .text(label);
        });
        // if(multiTimeSeriesObj.line1[2] == 'mean'){
        //     lengendG
        //         .append("rect")
        //         .attr("x", 0)
        //         .attr("y", keys.length * 25) 
        //         .attr("width", 20)
        //         .attr("height", 20)
        //         .attr("fill", colorArray[keys.length]);

        //     lengendG
        //         .append("text")
        //         .attr("x", 30) // 文本相对于颜色块的偏移
        //         .attr("y", keys.length * 25 + 15) // 与颜色块对齐并垂直居中
        //         .attr("font-size", "12px")
        //         .attr("fill", "#000")
        //         .text("Ave");
        // }
    }

    function draw(columnInfos?: any) {
        // const colorArray1 = ["#b3de69", "#fdb462", "#80b1d3", "#fb8072", "#bebada", "#ffffb3", "#8dd3c7", "#1f77b4", "#aec7e8", "#ff7f0e", "#ffbb78", "#2ca02c", "#98df8a", "#d62728", "#ff9896", "#9467bd", "#c5b0d5", "#8c564b", "#c49c94", "#e377c2", "#f7b6d2", "#7f7f7f", "#c7c7c7", "#bcbd22", "#dbdb8d", "#17becf", "#9edae5", "#393b79", "#5254a3", "#6b6ecf", "#9c9ede", "#637939", "#8ca252", "#b5cf6b", "#cedb9c", "#8c6d31", "#bd9e39", "#e7ba52", "#843c39", "#ad494a", "#d6616b", "#e7969c", "#7b4173", "#a55194", "#ce6dbd", "#de9ed6"];
        
        
        drawLengend(multiTimeSeriesObj.width + pading.left - 40, multiTimeSeriesObj, colorArray1)
        canvas.width = multiTimeSeriesObj.width;
        // const curMinMax = computeMinMax(multiTimeSeriesObj);
        yScale = d3.scaleLinear().domain([multiTimeSeriesObj.minv, multiTimeSeriesObj.maxv]).range([multiTimeSeriesObj.height, 0]);  
        yAxis = d3.axisLeft(yScale);
        // if (store.state.controlParams.currentMode === 'Default') {
        //     yAxis = d3.axisLeft(yScale).tickFormat((val) => {
        //         //@ts-ignore
        //         return 100 * val + "%"
        //     })
        // }
        if (yAxisG !== null && yAxisG !== undefined) {
            yAxisG.remove();
        }
        yAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${pading.top})`).attr("class", 'y axis').call(yAxis);

        showXTimeScale = d3.scaleTime().domain([new Date(Math.floor(indexToTimeStampScale(multiTimeSeriesObj.timeRange[0]))), new Date(Math.floor(indexToTimeStampScale(multiTimeSeriesObj.timeRange[1])))]).range([0, multiTimeSeriesObj.width]);
        // showXTimeScale = d3.scaleLinear().domain([0, rowNumber]).range([0, multiTimeSeriesObj.width]);
        xAxis = d3.axisBottom(showXTimeScale);
        if (xAxisG !== null && xAxisG !== undefined) {
            xAxisG.remove();
        }
        xAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${multiTimeSeriesObj.height + pading.top})`).attr("class", 'x axis').call(xAxis)
        
        columnInfos = multiTimeSeriesObj.columnInfos;
        ctx?.clearRect(0, 0, multiTimeSeriesObj.width, multiTimeSeriesObj.height);
        for (let i = 0; i < columnInfos.length; i++) {
            if (multiTimeSeriesObj.isShow[i]) {
                if (multiTimeSeriesObj.columnInfos[i]) {
                    ctx?.beginPath();
                    let color = colorMap.get(multiTimeSeriesObj.line1[multiTimeSeriesObj.line1.length-1][i]);
                    // if (store.state.controlParams.currentMode === 'Default') {
                    //     //ctx.strokeStyle = colorArray1[i];
                    //     const nonUniformColObjs = multiTimeSeriesObj.columnInfos[i];
                    //     let nameStrs = multiTimeSeriesObj.dataName[i].split(".")[1].split("_")
                    //     if (nameStrs[1][nameStrs[1].length - 1] !== 'r') {
                    //         nameStrs[1] = nameStrs[1] + 'r';
                    //     }
                    //     //@ts-ignore
                    //     ctx.strokeStyle = namedMap.get(nameStrs[1]).split("-")[1]//colorArray1[i];
                    // } else {
                        //@ts-ignore
                        // ctx.strokeStyle = colorArray1[multiTimeSeriesObj.columnInfos[i].md5Num! % 46];
                    // }
                    ctx.strokeStyle = color;
                    // ctx.strokeStyle = colorArray1[i];

                    //@ts-ignore
                    ctx.lineWidth = 1;
                    const nonUniformColObjs = columnInfos[i];
                    let interval = (nonUniformColObjs[0].et - nonUniformColObjs[0].st)/3;
                    let wholeInterval = nonUniformColObjs[0].et - nonUniformColObjs[0].st
                    for (let j = 0; j < nonUniformColObjs.length; j++) {
                        ctx?.moveTo(nonUniformColObjs[j].st/wholeInterval, yScale(nonUniformColObjs[j].sv));
                        ctx?.lineTo((nonUniformColObjs[j].st + interval)/wholeInterval, yScale(nonUniformColObjs[j].min));
                        ctx?.moveTo((nonUniformColObjs[j].st + interval)/wholeInterval, yScale(nonUniformColObjs[j].min));
                        ctx?.lineTo((nonUniformColObjs[j].st + interval*2)/wholeInterval, yScale(nonUniformColObjs[j].max));
                        ctx?.moveTo((nonUniformColObjs[j].st + interval*2)/wholeInterval, yScale(nonUniformColObjs[j].max));
                        ctx?.lineTo(nonUniformColObjs[j].et/wholeInterval, yScale(nonUniformColObjs[j].ev));
                        if (j <= nonUniformColObjs.length - 2) {
                            ctx?.moveTo(nonUniformColObjs[j].et/wholeInterval, yScale(nonUniformColObjs[j].ev));
                            ctx?.lineTo(nonUniformColObjs[j + 1].st/wholeInterval, yScale(nonUniformColObjs[j + 1].sv));
                        }
                    }
                    ctx?.stroke();
                    // if(multiTimeSeriesObj.line1[2] == 'mean'){
                    //     let ctx = canvas.getContext("2d");
                    //     //@ts-ignore
                    //     ctx.beginPath();
                    //     //@ts-ignore
                    //     ctx.lineWidth = 2;
                        
                    //     for (let j = nonUniformColObjs.length-1; j < nonUniformColObjs.length; j++) {
                    //         ctx?.moveTo(nonUniformColObjs[j].st/wholeInterval, yScale(nonUniformColObjs[j].sv));
                    //         ctx?.lineTo((nonUniformColObjs[j].st + interval)/wholeInterval, yScale(nonUniformColObjs[j].min));
                    //         ctx?.moveTo((nonUniformColObjs[j].st + interval)/wholeInterval, yScale(nonUniformColObjs[j].min));
                    //         ctx?.lineTo((nonUniformColObjs[j].st + interval*2)/wholeInterval, yScale(nonUniformColObjs[j].max));
                    //         ctx?.moveTo((nonUniformColObjs[j].st + interval*2)/wholeInterval, yScale(nonUniformColObjs[j].max));
                    //         ctx?.lineTo(nonUniformColObjs[j].et/wholeInterval, yScale(nonUniformColObjs[j].ev));
                    //         if (j <= nonUniformColObjs.length - 2) {
                    //             ctx?.moveTo(nonUniformColObjs[j].et/wholeInterval, yScale(nonUniformColObjs[j].ev));
                    //             ctx?.lineTo(nonUniformColObjs[j + 1].st/wholeInterval, yScale(nonUniformColObjs[j + 1].sv));
                    //         }
                    //     }
                    //     ctx?.stroke();
                    // }
                    // else{
                    //     for (let j = nonUniformColObjs.length-1; j < nonUniformColObjs.length; j++) {
                    //         ctx?.moveTo(nonUniformColObjs[j].st/wholeInterval, yScale(nonUniformColObjs[j].sv));
                    //         ctx?.lineTo((nonUniformColObjs[j].st + interval)/wholeInterval, yScale(nonUniformColObjs[j].min));
                    //         ctx?.moveTo((nonUniformColObjs[j].st + interval)/wholeInterval, yScale(nonUniformColObjs[j].min));
                    //         ctx?.lineTo((nonUniformColObjs[j].st + interval*2)/wholeInterval, yScale(nonUniformColObjs[j].max));
                    //         ctx?.moveTo((nonUniformColObjs[j].st + interval*2)/wholeInterval, yScale(nonUniformColObjs[j].max));
                    //         ctx?.lineTo(nonUniformColObjs[j].et/wholeInterval, yScale(nonUniformColObjs[j].ev));
                    //         if (j <= nonUniformColObjs.length - 2) {
                    //             ctx?.moveTo(nonUniformColObjs[j].et/wholeInterval, yScale(nonUniformColObjs[j].ev));
                    //             ctx?.lineTo(nonUniformColObjs[j + 1].st/wholeInterval, yScale(nonUniformColObjs[j + 1].sv));
                    //         }
                    //     }
                    //     ctx?.stroke();
                    // }
                    
                }
            }
        }
        // savePNG(canvas);
        // savePNG(document.getElementById("content-container"));
    }

    async function resizeW(width: number) {
        isResizing = true;
        //timeboxStack = [];
        const currentLevel = multiTimeSeriesObj.currentLevel;
        // multiTimeSeriesObj.currentLevel = 10;//currentLevel + 1
        multiTimeSeriesObj.width = width;
        updateCanvasWidth();

        if (currentLevel + 1 >= multiTimeSeriesObj.maxLevel - 1) {
            return
        }

        //@ts-ignore
        canvas.style.width = multiTimeSeriesObj.width

        console.log("resize....");
        // let mode = "show";
        // let type = ""
        // let parallel = 0;
        // let errorBound = 0;
        // let line1 = multiTimeSeriesObj.line1;
        // const combinedUrl = `/line_chart/case1?table_name=${line1[0]}&table_name_others=${line1[1]}&symbol=${line1[2]}&mode=${mode}&width=${multiTimeSeriesObj.width}&height=${multiTimeSeriesObj.height}&startTime=${multiTimeSeriesObj.timeRange[0]}&endTime=${multiTimeSeriesObj.timeRange[1]}&interact_type=${type}&experiment=${line1[3]}&parallel=${parallel}&errorBound=${errorBound}`;
        // const showColumns = await get(combinedUrl);
        // multiTimeSeriesObj.columnInfos = showColumns;
        // draw();
    }

    async function zoomIn(timeRange: Array<number>) {
        //timeboxStack = [];
        const currentLevel = multiTimeSeriesObj.currentLevel;
        multiTimeSeriesObj.currentLevel = 10;//currentLevel + 1
        const width = multiTimeSeriesObj.width;
        multiTimeSeriesObj.timeRange[1] = timeRange[1]//Math.floor(multiTimeSeriesObj.timeRange[1] / 2);
        multiTimeSeriesObj.timeRange[0] = timeRange[0]
        if (currentLevel + 1 >= multiTimeSeriesObj.maxLevel - 1) {
            return
        }

        const needLoadLevel = 2 ** Math.ceil(Math.log2(width))
        console.log("zoomIn....");
        let mode = "show";
        let type = ""
        let parallel = 0;
        let errorBound = 0;
        let line1 = multiTimeSeriesObj.line1;
        const combinedUrl = `/line_chart/case1?table_name=${line1[0]}&table_name_others=${line1[1]}&symbol=${line1[2]}&mode=${mode}&width=${multiTimeSeriesObj.width}&height=${multiTimeSeriesObj.height}&startTime=${multiTimeSeriesObj.timeRange[0]}&endTime=${multiTimeSeriesObj.timeRange[1]}&interact_type=${type}&experiment=${line1[3]}&parallel=${parallel}&errorBound=${errorBound}`;
        const showColumns = await get(combinedUrl);
        multiTimeSeriesObj.columnInfos = showColumns;
        draw();
    }


    //@ts-ignore
    async function brushed({ selection }) {
        if (!isInit) {
            isInit = true
            return;
        }
        if (isResizing) {
            isRebacking = false;
            return;
        }
        if (isRebacking) {
            isRebacking = false;
            return
        }
        const timeRange = [Math.floor(xReScale(selection[0])), Math.floor(xReScale(selection[1]))];
        if (timeRange[0] < 0) {
            timeRange[0] = 0;
        }
        if (timeRange[1] > nodeIndexRange[1]) {
            timeRange[1] = nodeIndexRange[1]
        }
        const interInfo = new InteractionInfo("zoom")
        interInfo.setRangeW(multiTimeSeriesObj.timeRange, multiTimeSeriesObj.width, multiTimeSeriesObj.currentLevel);
        interactionStack.push(interInfo);
        zoomIn([timeRange[0], timeRange[1]])
    }

    let timeboxStack: Array<Array<boolean>> = [];

    //@ts-ignore
    function timeBoxBrushed({ selection }) {
        if (selection === null) {
            return;
        }
        const lastRes = [];
        for (let i = 0; i < multiTimeSeriesObj.columnInfos.length; i++) {
            lastRes.push(multiTimeSeriesObj.isShow[i]);
        }
        //if(interactionStack[interactionStack.length-1].type==='timebox')
        const timeBoxStartTime = new Date().getTime();
        const interInfo = new InteractionInfo("timebox");
        interInfo.setShowInfo(lastRes);
        interactionStack.push(interInfo);
        // console.log(selection);
        const startX = selection[0][0] - 40;
        const endX = selection[1][0] - 40;
        // console.log(showTimeXScale.invert(startX));
        // console.log(showTimeXScale.invert(endX));
        const vMinY = selection[1][1] - 20;
        const vMaxY = selection[0][1] - 20;
        const vMin = yScale.invert(vMinY);
        const vMax = yScale.invert(vMaxY);
        const allColumnInfos = multiTimeSeriesObj.columnInfos;

        for (let i = 0; i < allColumnInfos.length; i++) {
            let isChoose = multiTimeSeriesObj.isShow[i];
            for (let j = Math.floor(startX); j <= Math.floor(endX); j++) {
                isChoose = isChoose && (allColumnInfos[i][j].min >= vMin && allColumnInfos[i][j].max <= vMax)
            }
            multiTimeSeriesObj.isShow[i] = false;
            if (isChoose) {
                multiTimeSeriesObj.isShow[i] = true
                //console.log(multiTimeSeriesObj.dataManagers[i].dataName);
            }
            // multiTimeSeriesObj.isShow[i] = false;
        }
        let minV = Infinity;
        let maxV = -Infinity;
        for (let i = 0; i < allColumnInfos.length; i++) {
            if (multiTimeSeriesObj.isShow[i]) {
                for (let j = 0; j <= allColumnInfos[i].length; j++) {
                    minV = Math.min(minV, allColumnInfos[i][j] ? allColumnInfos[i][j].vRange[0] : minV);

                    maxV = Math.max(maxV, allColumnInfos[i][j] ? allColumnInfos[i][j].vRange[1] : maxV);
                }
            }
        }
        // multiTimeSeriesObj.maxv = -10;
        // multiTimeSeriesObj.minv = 10
        draw();
        //zoomIn([timeRangeScale(1493740845000),timeRangeScale(1493805314000)])
    }
    let isMouseover = false;
    let startOffsetX = 0;
    const dragRect = svg
        .append("rect")
        .attr("x", 0)
        .attr("y", 0)
        .attr("height", multiTimeSeriesObj.height + pading.top + pading.bottom)
        .attr("width", multiTimeSeriesObj.width + pading.left + pading.right)
        .attr("stroke", "black")
        .attr("stroke-width", 4)
        .attr("fill", "none")
        .on("mouseover", () => {
            document.body.style.cursor = 'ew-resize';
            isMouseover = true;
        })
        .on("mousedown", (e) => {
            if (isMouseover) {
                startOffsetX = e.offsetX;
                interactiveInfo.isMouseDown = true;
            }
        })
        .on("mouseup", () => {
            console.log();
            for (let i = 0; i < 30; i++) {
                multiTimeSeriesObj.isShow[i] = true;
            }
        })
        .on("mousemove", (e) => {
            if (interactiveInfo.isMouseDown) {
                svg.attr("width", multiTimeSeriesObj.width + pading.right + pading.left + (e.offsetX - startOffsetX));
                dragRect.attr("width", multiTimeSeriesObj.width + pading.right + pading.left + (e.offsetX - startOffsetX));
            }
        })
        .on("mouseleave", () => {
            if (!interactiveInfo.isMouseDown) {
                document.body.style.cursor = 'default';
            }
            for (let i = 0; i < 30; i++) {
                multiTimeSeriesObj.isShow[i] = true;
            }
        })

    document
        .getElementById("content-container")!
        .addEventListener("mousemove", (e) => {
            if (interactiveInfo.isMouseDown) {
                svg.attr("width", multiTimeSeriesObj.width + pading.right + pading.left + (e.offsetX - startOffsetX));
                dragRect.attr("width", multiTimeSeriesObj.width + pading.right + pading.left + (e.offsetX - startOffsetX));
            }
        });

    document
        .getElementById("content-container")!
        .addEventListener("mouseup", (e) => {
            if (interactiveInfo.isMouseDown) {
                interactiveInfo.isMouseDown = false;
                isMouseover = false;
                document.body.style.cursor = 'default';
                let preWidth = multiTimeSeriesObj.width;
                multiTimeSeriesObj.width = multiTimeSeriesObj.width + (e.offsetX - startOffsetX);
                if (multiTimeSeriesObj.width === preWidth) {
                    return
                }
                const interInfo = new InteractionInfo("resize")
                interInfo.setRangeW(multiTimeSeriesObj.timeRange, multiTimeSeriesObj.width, multiTimeSeriesObj.currentLevel);
                interactionStack.push(interInfo);
                resizeW(multiTimeSeriesObj.width)
            }
            for (let i = 0; i < 30; i++) {
                multiTimeSeriesObj.isShow[i] = true;
            }
        });
    svg.on("contextmenu", (e) => {

        if (interactionStack.length > 0) {
            const interInfo = interactionStack.pop();

            if (interInfo?.type === 'timebox') {
                const curStats = interInfo.showInfo;
                for (let i = 0; i < multiTimeSeriesObj.columnInfos.length; i++) {
                    multiTimeSeriesObj.isShow[i] = curStats![i];
                }
                draw();
            } else if (interInfo?.type === 'resize') {

                resizeW(interInfo.width);
            } else if (interInfo?.type === 'zoom') {
                isRebacking = true;
                zoomIn(interInfo.timeRange)
            }
        }
        e.preventDefault();
        e.stopPropagation();
    });


    let i = 0;
    canvas.addEventListener("click", (e) => {
        console.log(1);
    });
    draw();
}

function computeMinMax(multiTimeSeriesObj: MultiTimeSeriesObj) {
    let min = Infinity;
    let max = -Infinity;
    // for (let i = 0; i < multiTimeSeriesObj.columnInfos.length; i++) {
    //     if (multiTimeSeriesObj.isShow[i]) {
    //         const columInfo = multiTimeSeriesObj.columnInfos[i];
    //         for (let j = 0; j < columInfo.length; j++) {
    //             if (columInfo[j].vRange[0] < min) {
    //                 min = columInfo[j].vRange[0];
    //             }

    //             if (columInfo[j].vRange[1] > max) {
    //                 max = columInfo[j].vRange[1];
    //             }
    //         }
    //     } else {
    //         // console.log(multiTimeSeriesObj.dataManagers[i].dataName)
    //     }
    // }
    min = -2000, max = 2000;
    return {
        min,
        max
    }
}

function savePNG(canvas: any) {
    const imgURL = canvas.toDataURL("./image/png");
    const dlLink = document.createElement("a")
    dlLink.download = `1b_zoom_init`
    dlLink.href = imgURL
    dlLink.dataset.downloadurl = ["./image/png", dlLink.download, dlLink.href].join(":")
    document.body.appendChild(dlLink)
    dlLink.click()
    document.body.removeChild(dlLink)
}


        // let showNum = 0;
        // for (let i = 0; i < multiTimeSeriesObj.columnInfos.length; i++) {
        //     const dataManager = multiTimeSeriesObj.columnInfos[i];
        //     if (dataManager.isShow) {
        //         if (store.state.controlParams.currentMode === 'Default') {
        //             let nameStrs = dataManager.dataName.split(".")[1].split("_")
        //             if (nameStrs[1][nameStrs[1].length - 1] !== 'r') {
        //                 nameStrs[1] = nameStrs[1] + 'r';
        //             }
        //             if (!namedMap.has(nameStrs[1])) {
        //                 dataManager.isShow = false;
        //                 continue
        //             }
        //             showNum++;
        //         } else {
        //             let nameStrs = dataManager.dataName.split(".")[1].split("_");
        //             let showName = nameStrs[1]
        //             for (let i = 2; i < nameStrs.length - 2; i++) {
        //                 showName = showName + "_" + nameStrs[i];
        //             }
        //             const showColor = colorArray[dataManager.md5Num! % 46];
        //             showNum++;
        //         }
        //     }
        // }