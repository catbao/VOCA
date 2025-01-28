<template>
  <div class="data-control-panel radius">

    <div class="d-flex ms-2" marginTop="10px">
        <input type="number" class="form-control form-control-sm dim-input" v-model="widthRef" />
        <span style="line-height: 31px">×</span>
        <input type="number" class="form-control form-control-sm dim-input" v-model="heightRef" />
      </div>  
    <div class="choose_container choose_mode_container ms-1">
      <el-radio-group v-model="chooseMode" size="medium" @change="handleModeChange">
        <el-radio-button label="Default"></el-radio-button>
        <el-radio-button label="Custom"></el-radio-button>
      </el-radio-group>
    </div>


    <div class="dbsetting-control-container mt-2" v-if="chooseMode === 'Custom'">
      <div>
        <el-switch v-model="isOpenDbSetup" name="DB Setup" />
        <label>{{ "DB Setup" }}</label>
      </div>
      <div class="mt-2 " v-if="isOpenDbSetup">
        <el-input v-model="customDBHostName" type="string" placeholder="DB Host Name" />
        <el-input v-model="customUserName" type="string" placeholder="User Name" />
        <el-input v-model="customDBPassword" type="password" placeholder="DB Password" />
        <el-input v-model="customDBName" type="string" placeholder="DB Name" />
        <div><el-button class="btn mt-2" v-on:click="handleTestConn" type="primary">{{ testConnectResult }}</el-button>
        </div>
        <div><el-button class="btn mt-2 " v-on:click="createCustomDBConn" type="primary">{{ createDBConn }}</el-button>
        </div>
        <div><el-button class="btn mt-2" v-on:click="initOM3DBEnv" type="primary">{{ initOm3DB }}</el-button></div>
        <div>
          <el-popconfirm title="This will clear all om3 table, are you sure you want to do this?"
            @confirm="clearOM3DBEnv">
            <template #reference>
              <el-button class="btn mt-2" type="primary">{{ clearOm3DB }}</el-button>
            </template>
          </el-popconfirm>
        </div>
        <div><el-button class="btn mt-2" v-on:click="clearIndexFlag" type="primary">Clear Flag</el-button></div>
      </div>
    </div>

    <div class="om3-transform-control-container mt-2" v-if="chooseMode === 'Custom'">
      <div>
        <el-switch v-model="isOpenTransform" name="OM3 Transform" />
        <label>{{ "Data Transform" }}</label>
      </div>
      <div class="mt-2 " v-if="isOpenTransform">

        <div><el-button class="btn mt-2" text @click="dialogFormVisible = true" type="primary"
            v-if="chooseLineType === 'Single'">Choose Data</el-button>
        </div>
        <el-dialog v-model="dialogFormVisible" title="Single Line Transform">
          <label>Please choose a table,which inclues two columns(t interger,v double precision)(t should be a positive
            integer starting from zero)</label>
          <el-form>
            <el-form-item label="Table">
              <el-select v-model="singleLineTableName" placeholder="Please select a table for transform">
                <el-option v-for="(item, idx) in allCustomDBTables" :key="idx" :label="item" :value="item"></el-option>
              </el-select>
            </el-form-item>
            <label>Please select a date and time, which will be linearly mapped to t during visualization </label>
            <el-form-item label="Date Range">
              <el-date-picker v-model="lineDateRange" type="daterange" start-placeholder="Start Date"
                end-placeholder="End Date" :default-value="[new Date(2010, 9, 1), new Date(2010, 10, 1)]" />
            </el-form-item>
            <el-form-item label="Time Range">
              <div class="example-basic">
                <el-time-picker v-model="lineStartTime" arrow-control placeholder="Start Time" />
                <el-time-picker v-model="lineEndTime" arrow-control placeholder="End Time" />
              </div>
            </el-form-item>
          </el-form>
          <template #footer>
            <span class="dialog-footer">
              <el-button @click="dialogFormVisible = false">Cancel</el-button>
              <el-button type="primary" v-loading.fullscreen.lock="fullscreenLoading"
                v-on:click="performTransformForSingeLine">
                Confirm
              </el-button>
            </span>
          </template>
        </el-dialog>


        <div><el-button class="btn mt-2" text @click="multiLineTransformDialogVisible = true" type="primary"
            v-if="chooseLineType === 'Multi'">Choose Data</el-button>
        </div>
        <el-dialog v-model="multiLineTransformDialogVisible" title="Multi Line Transform">

          <label>Please choose tables,which inclues two columns(t interger,v double precision)(t should be a positive
            integer starting from zero). All the data you choose should be the same length</label>
          <el-form>
            <el-form-item label="Table">
              <el-select class="multi-line-select" v-model="multiLineTableNames" multiple
                placeholder="Please select table for transform">
                <el-option v-for="(item, idx) in allCustomDBTables" :key="idx" :label="item" :value="item"></el-option>
              </el-select>
            </el-form-item>
            <el-input class="mb-4" v-model="customMultiLineClassName"
              placeholder="Please define a group for the lines you select" clearable />
            <label>Please select a date and time, which will be linearly mapped to t during visualization </label>
            <el-form-item label="Date Range">
              <el-date-picker v-model="lineDateRange" type="daterange" start-placeholder="Start Date"
                end-placeholder="End Date" :default-value="[new Date(2010, 9, 1), new Date(2010, 10, 1)]" />
            </el-form-item>
            <el-form-item label="Time Range">
              <div class="example-basic">
                <el-time-picker v-model="lineStartTime" arrow-control placeholder="Start Time" />
                <el-time-picker v-model="lineEndTime" arrow-control placeholder="End Time" />
              </div>
            </el-form-item>
          </el-form>
          <template #footer>
            <span class="dialog-footer">
              <el-button @click="multiLineTransformDialogVisible = false">Cancel</el-button>
              <el-button type="primary" v-loading.fullscreen.lock="fullscreenLoading"
                v-on:click="perfromTransformForMulitLine">
                Confirm
              </el-button>
            </span>
          </template>
        </el-dialog>

      </div>
    </div>

    <!-- <div class="choose_container choose_line_type_container ms-1">
      <el-radio-group v-model="chooseLineType" size="medium" @change="handleLineTypeChange">
        <el-radio-button label="Single"></el-radio-button>
        <el-radio-button label="Multi"></el-radio-button>
      </el-radio-group>
    </div> -->

    <!-- <div class="progressive-container ms-1 mt-2" v-if="chooseLineType==='Single'">
      <span>Progressive</span>
        <el-switch v-model="progressive" name="Progressive"  @change="handleProgressiveChange"/>
    </div> -->



    <div class="class-choose-container mt-2 ms-1" >
      <el-select v-model="currentMultiClass" placeholder="Select" size="medium" @change="handleMultiLineClassChange"
      @focus="onFocus">
      <el-option v-for="(item, idx) in multiLineClassAndLinesMap.get('tables')" :key="idx" :label="item" :value="item">
      </el-option>
      </el-select>
    </div>

    <div class="class-choose-container2 mt-2 ms-1">
      <el-select v-model="currentMultiClassLines" multiple
      placeholder="Select" size="medium" @change="handleColumnsChange"
      @focus="onFocusColumns">
      <el-option v-for="(item, idx) in multiLineClassAndLinesMap.get('columns')" :key="idx" :label="item" :value="item">
      </el-option>
      </el-select>   
    </div>

    <div class="compute-line-container mt-2 ms-1">
      <el-select v-model="selectedOption" placeholder="Operator" @change="handleSelectedOption" @focus="onFocusFunctions">
        <el-option v-for="(item, idx) in multiLineClassAndLinesMap.get('functions')" :key="idx" :label="item" :value="item">
        </el-option>
      </el-select>
    </div>

    <div class="compute-line-container mt-2 ms-1">
      <el-select v-model="selectedAggregate" placeholder="Aggregate" @change="handleSelectedAggregate" @focus="onFocusAggregate">
        <el-option v-for="(item, idx) in multiLineClassAndLinesMap.get('aggregates')" :key="idx" :label="item" :value="item">
        </el-option>
      </el-select>
    </div>


    <div class="mt-2 mb-1">
      <el-select v-model="selectedExperiment" placeholder="Experiment" @change="handleSelectedExperiment" @focus="onFocusExperiment">
        <el-option v-for="(item, idx) in multiLineClassAndLinesMap.get('experiments')" :key="idx" :label="item" :value="item">
        </el-option>
      </el-select>
    </div>


    <div class="mt-2 mb-1">
        <el-input v-model="errorBound" placeholder="errorBound" />
    </div>




    <!-- <div class="mt-2 mb-1">
      <el-select v-model="selectedComputeOrShow" placeholder="Compute/Show" @change="handleSelectedComputeOrShow">
        <el-option label="compute" value="compute"></el-option>
        <el-option label="show" value="show"></el-option>
      </el-select>
    </div> -->






    <div class="class-choose-container2 mt-2 ms-1">
      <el-select v-model="currentMultiClassALine" placeholder="Select" size="medium" @change="handleMultiLineClassALineChange"
        v-if="chooseMode === 'Custom'">
        <el-option v-for="(item, idx) in multiLineClassAndLinesMap.get('mock')" :key="idx" :label="item" :value="item">
        </el-option>
      </el-select>   
    </div>


    <!-- <div class="table-choose-container mt-2 ms-1">
      <el-select v-model="currentTable" placeholder="Select" size="medium" v-if="chooseMode === 'Default'"
        @change="handleTableChange">
        <el-option v-for="(item, idx) in allDefaultTables" :key="idx" :label="item" :value="item">
        </el-option>
      </el-select>
      <el-select v-model="currentTable" placeholder="Select" size="medium" v-on:click="loadCustomTableAndInfo"
        v-if="chooseMode === 'Custom'" @change="handleTableChange">
        <el-option v-for="(item, idx) in allCustomTables" :key="idx" :label="item" :value="item">
        </el-option>
      </el-select>
    </div> -->




    <!-- <div class="mt-2 mb-1" v-if="chooseLineType == 'Multi'">
        <el-switch v-model="isStopEarly" name="OM3 StopEarly" />
        <label>{{ "Stop Early" }}</label>
    </div> -->

    <div>
      <button id="create_panel_btn" type="button" class="btn btn-secondary ms-2 mt-2 ml-4" style="width: 100px; height: 40px;" @click.prevent="handleComputePanel">
        create
      </button>
    </div>

    <!-- <div>
      <button id="create_panel_btn2" type="button" class="btn btn-secondary ms-2 mt-2 ml-4" style="width: 100px; height: 40px;" @click.prevent="handleExperiment">
        Experiment
      </button>
    </div> -->

    <!-- <div>
      <button id="create_panel_btn2" type="button" class="btn btn-secondary ms-2 mt-2 ml-4" style="width: 100px; height: 40px;" @click.prevent="handleAddAverage">
        add Ave
      </button>
    </div> -->


  </div>
</template>
<script>
import store from "@/store";
import { defineComponent, ref, computed, watch, reactive } from "vue";
import { ElLoading } from 'element-plus'
import { ElNotification } from 'element-plus'
import { clearFlagDB } from "@/indexdb";
import axios from "axios";

async function get(url) {
  url = 'postgres' + url;
  //const loading = openLoading();
  const { data } = await axios.get(url);
  //loading.close();
  return data;
}

export default defineComponent({
  data() {
    return {
      testConnectResult: "Test Connection",
      createDBConn: "Create Connection",
      initOm3DB: "Init OM3 DB",
      clearOm3DB: "Clear OM3 DB",
      customDBHostName: "",
      customDBPassword: '',
      customDBName: '',
      customUserName: "",
      isOpenDbSetup: false,
      isOpenTransform: false,
      isStopEarly: false,
      lineDateRange: [new Date(2010, 9, 1), new Date(2010, 10, 1)],
      lineStartTime: new Date(2010, 9, 1),
      lineEndTime: new Date(2010, 10, 1),
      fullscreenLoading: false,
      singleLineTableName: "",
      multiLineTableNames: [],
      customMultiLineClassName: "",
    }

  },
  components: {

  },
  methods: {
    onFocus(){
      this.$store.dispatch("gettables");
    },

    onFocusColumns(){
      this.$store.dispatch("getcolumns");
    },

    onFocusFunctions(){
      this.$store.dispatch("getfunctions");
    },

    onFocusAggregate(){
      this.$store.dispatch("getaggregates");
    },

    onFocusExperiment(){
      this.$store.dispatch("getexperiment");
    },

    updateTestConnRes(res) {
      this.testConnectResult = res
    },
    updateDBCreateConn(res) {
      this.createDBConn = res;
    },
    updateInitDB(res) {
      this.initOm3DB = res
    },
    updateClearDB(res) {
      this.clearOm3DB = res;
    },
    openFullScreenLoading() {
      this.fullscreenLoading = true;
    },
    closeFullScreenLoading() {
      this.fullscreenLoading = false;
    },
    openNotification(title, msg, type) {
      ElNotification({
        title: title,
        message: msg,
        type: type,
      })
    },
    handleTestConn() {
      store.dispatch("testCustomDBConn", { hostName: this.customDBHostName, possword: this.customDBPassword, dbName: this.customDBName, userName: this.customUserName }).then((res) => {
        const result = res.data['data']['result'];
        if (result === 'success') {
          this.storeDBConfig()
          this.updateTestConnRes("Test Success")
          this.openNotification("Test Connection", "Test Connection Success", "success")
        } else {

          console.error(res.data['msg'])
          this.updateTestConnRes("Test Fail")
          this.openNotification("Test Connection", res.data['msg'], "error")
        }
      })
    },
    storeDBConfig() {
      localStorage.setItem("customDBHostName", this.customDBHostName)
      localStorage.setItem("customDBPassword", this.customDBPassword)
      localStorage.setItem("customDBName", this.customDBName)
      localStorage.setItem("customUserName", this.customUserName)
    },
    restoreDBConfig() {
      const customDBHostName = localStorage.getItem("customDBHostName")
      const customDBPassword = localStorage.getItem("customDBPassword");
      const customDBName = localStorage.getItem("customDBName");
      const customUserName = localStorage.getItem("customUserName");
      if (customDBHostName) {
        this.customDBHostName = customDBHostName;
      }
      if (customDBPassword) {
        this.customDBPassword = customDBPassword;
      }
      if (customDBName) {
        this.customDBName = customDBName;
      }
      if (customUserName) {
        this.customUserName = customUserName;
      }
    },
    createCustomDBConn() {
      store.dispatch("createCustomDBConn", { hostName: this.customDBHostName, possword: this.customDBPassword, dbName: this.customDBName, userName: this.customUserName }).then((res) => {
        const result = res.data['data']['result'];
        if (result === 'success') {
          this.updateDBCreateConn("Create Success")
          this.openNotification("Create Connection", "Create Connection Success", "success")
        } else {

          console.error(res.data['msg'])
          this.updateDBCreateConn("Create Fail")
          this.openNotification("Create Connection", res.data['msg'], "error")
        }
      })
    },
    initOM3DBEnv() {
      store.dispatch("initOM3DB").then((res) => {
        console.log(res)
        const result = res.data['result'];
        if (result === 'success') {
          this.updateInitDB("Init Success")
          this.openNotification("Init OM3 ENV", "Init OM3 ENV Success", "success")
        } else {

          console.error(res.data['msg'])
          this.updateInitDB("Init Fail")
          this.openNotification("Init OM3 ENV", res.data['msg'], "error")
        }
      })
    },
    clearOM3DBEnv() {
      console.log("clear om3 env")
      store.dispatch("clearOM3Table").then((res) => {
        console.log(res)
        const result = res.data['result'];
        if (result === 'success') {
          this.updateClearDB("Clear Success")
          this.openNotification("Clear OM3 ENV", "Clear OM3 ENV Success", "success")
        } else {

          console.error(res.data['msg'])
          this.updateClearDB("Clear Fail")
          this.openNotification("Clear OM3 ENV", res.data['msg'], "error")
        }
      })
    },
    performTransformForSingeLine() {
      let startDateStr = this.lineDateRange[0].toISOString().split("T")[0];
      let endDateStr = this.lineDateRange[1].toISOString().split("T")[0];
      let startTimeStr = this.lineStartTime.toISOString().split("T")[1].split('Z')[0];
      let endTimeStr = this.lineEndTime.toISOString().split("T")[1].split('Z')[0];
      const startFullTime = startDateStr + " " + startTimeStr;
      const endFullTime = endDateStr + " " + endTimeStr;
      this.openFullScreenLoading();
      store.dispatch("performTransformForSingeLine", { startTime: startFullTime, endTime: endFullTime, tableName: this.singleLineTableName }).then((res) => {
        console.log("res:"+res.data);
        if (res.data['code'] === 200) {
          console.log("single line transform success")
          //  add tishi
        } else {
          console.error(res.data['msg'])
        }
        this.closeFullScreenLoading()
        this.dialogFormVisible = false;
      })
    },
    perfromTransformForMulitLine() {
      let startDateStr = this.lineDateRange[0].toISOString().split("T")[0];
      let endDateStr = this.lineDateRange[1].toISOString().split("T")[0];
      let startTimeStr = this.lineStartTime.toISOString().split("T")[1].split('Z')[0];
      let endTimeStr = this.lineEndTime.toISOString().split("T")[1].split('Z')[0];
      const startFullTime = startDateStr + " " + startTimeStr;
      const endFullTime = endDateStr + " " + endTimeStr;
      this.openFullScreenLoading()
      store.dispatch("performTransformForMultiLine", { startTime: startFullTime, endTime: endFullTime, tableNames: Array.from(this.multiLineTableNames.values()), multiLineClassName: this.customMultiLineClassName }).then((res) => {
        if (res['code'] === 200) {
          console.log("multi line transform success")
          //  add tishi
        } else {
          console.error(res['msg'])
        }
        this.closeFullScreenLoading()
        this.multiLineTransformDialogVisible = false;
      });
      store.commit("setAllMultiLineClassAndLinesMap",{
        key: this.customMultiLineClassName,
        value: Array.from(this.multiLineTableNames.values())
      });
      // console.log("computeAllMultiLineClassAndLinesMap:", store.state.allMultiLineClassAndLinesMap['bao'])
      // console.log("computeAllMultiLineClassAndLinesMap:", store.state.allMultiLineClassAndLinesMap['bao'])
      console.log(startFullTime, endFullTime, this.customMultiLineClassName, Array.from(this.multiLineTableNames.values()))
    }
  },

  watch: {
    isOpenDbSetup(newV, oldV) {
      if (newV) {
        this.restoreDBConfig()
      } else {
        this.updateDBCreateConn("Create Connection")
        this.updateTestConnRes("Test Connection")
      }
    },
    isOpenTransform(newV, oldV) {
      if (newV) {
        store.dispatch("getAllCustomTables").then((res) => {
          if (res['code'] === 200) {
            this.allCustomDBTables = res['data']['result'];
          } else {
            console.error(res['msg'])
          }
        })
      }
    },
    isStopEarly(newV, oldV){
      if(oldV === true){
        store.commit("alterNoStopEarly", false);
        // console.log("store.state.controlParams.StopEarly:", store.state.controlParams.StopEarly);
      }
      else if(newV){
        // console.log("newV:", newV);
        store.commit("alterStopEarly", true);
        // console.log("store.state.controlParams.StopEarly:", store.state.controlParams.StopEarly);
      }
      // else{
      //   store.commit("alterStopEarly", false);
      // }
    }
  },
  setup() {
    // let tableArray = store.state.allMultiLineClassAndLinesMap.get('bao');
    // console.log("computeAllMultiLineClassAndLinesMap:", store.state.allMultiLineClassAndLinesMap);
    const progressive = ref(store.state.controlParams.progressive);
    const chooseMode = ref("Default");
    const chooseLineType = ref("Single");
    const dialogFormVisible = ref(false);

    const multiLineTransformDialogVisible = ref(false)

    const allAlgoritem = store.state.allAlgoritem;

    const currentSampleAlgorithm = ref(
      store.state.controlParams.currentSampleMethod
    );

    const currentTable = ref(store.state.controlParams.currentTable);

    const currentCustomTable = ref(store.state.controlParams.currentCustomTable)
    const currentMultiClass = ref(store.state.controlParams.currentMultiLineClass);
    const currentMultiClassALine = ref(store.state.controlParams.currentMultiLineClassALine);
    const currentMultiClassLines = ref(store.state.controlParams.currentMultiLineClassLines);
    const selectedOption = ref(store.state.controlParams.transform_symbol);
    const selectedExperiment = ref(store.state.controlParams.experiment);
    const selectedAggregate = ref(store.state.controlParams.aggregate);
    const selectedComputeOrShow = ref(store.state.controlParams.computeOrShow);
    // const multiLineClassAndLinesMap = ref(store.state.allMultiLineClassAndLinesMap);

    const widthRef = ref(600);
    const heightRef = ref(600);
    const errorBound = ref(0.05);
    const currentDB = computed(() => {
      return store.state.controlParams.currentDB;
    });

    const handleExperiment = () => {
      // console.log("currentMultiClassALine:",currentMultiClassALine.value);
      // console.log(Array.from(currentMultiClassLines.value));
      const payload = {
          width: widthRef.value,
          height: heightRef.value,
      };
      store.dispatch("Experiment", [currentMultiClass.value, Array.from(currentMultiClassLines.value), selectedOption.value, selectedExperiment.value, payload, errorBound, selectedComputeOrShow.value, selectedAggregate.value]);
    }

    const handleAddAverage = () => {
      // console.log("currentMultiClassALine:",currentMultiClassALine.value);
      // console.log(Array.from(currentMultiClassLines.value));
      const payload = {
          width: widthRef.value,
          height: heightRef.value,
      };
      store.dispatch("computeLineTransform", [currentMultiClass.value, Array.from(currentMultiClassLines.value), selectedOption.value,selectedExperiment.value, payload, errorBound, 'compute', selectedAggregate.value]);
    }


    const handleComputePanel = () => {
      // console.log("currentMultiClassALine:",currentMultiClassALine.value);
      // console.log(Array.from(currentMultiClassLines.value));
      const payload = {
          width: widthRef.value,
          height: heightRef.value,
      };
      store.dispatch("computeLineTransform", [currentMultiClass.value, Array.from(currentMultiClassLines.value), selectedOption.value,selectedExperiment.value, payload, errorBound, 'compute', selectedAggregate.value, store.state.controlParams.startTime, store.state.controlParams.endTime]);
    }

    const allSampleAlgoritem = store.state.controlParams.sampleMethods;

    let allTables = computed(() => {
      return store.state.allTables;
    });
    const allCustomTables = computed(() => {
      return store.state.allCustomTables;
    })

    const allDefaultTables = computed(() => {
      return store.state.allDefaultTables;
    })

    let allMultLineClass = computed(() => {
      return store.state.allMultiLineClassInfoMap
    })

    let allCustomMultiLineClass = computed(() => {
      return store.state.allCustomMultiLineClassInfoMap
    })

    let multiLineClassAndLinesMap = computed(() => {
      // console.log("computeAllMultiLineClassAndLinesMap:", store.state.allMultiLineClassAndLinesMap)
      // const tableArray = store.state.allMultiLineClassAndLinesMap.get("bao");
      // console.log(Array.from(tableArray));
      // return tableArray;
      return store.state.allMultiLineClassAndLinesMap;
    })

    const handleSampleMethodChange = () => {
      store.commit("alterSampleMethod", currentSampleAlgorithm.value);
    };

    const handleSelectedOption = () => {
      console.log("Current Symbol:", selectedOption.value);
      store.commit("alterSelectedOption", selectedOption.value);
    }

    const handleSelectedExperiment = () => {
      store.commit("alterSelectedExperiment", selectedExperiment.value);
    }

    const handleSelectedAggregate = () => {
      store.commit("alterSelectedAggregate", selectedAggregate.value);
    }

    const handleSelectedComputeOrShow = () => {
      store.commit("alterSelectedComputeOrShow", selectedComputeOrShow.value);
    }

    const handleModeChange = () => {
      store.commit("alterMode", chooseMode.value);
      chooseLineType.value = "Single"
      store.commit("alterLineType", chooseLineType.value);
    }
    const handleLineTypeChange = () => {
      store.commit("alterLineType", chooseLineType.value)
      // store.dispatch("getAllMultiLineClassInfo");
      // store.dispatch("getAllMultiLineClassAndLinesInfo");
      // console.log("computeAllMultiLineClassInfo:", store.state.allMultiLineClassInfoMap);
      // console.log("computeAllMultiLineClassAndLinesMap:", store.state.allMultiLineClassAndLinesMap.get("bao"));
      // const arr = Array.from(store.state.allMultiLineClassAndLinesMap.get("bao"));
      // console.log(arr);
    }
    const handleMultiLineClassChange = () => {
      console.log(currentMultiClass.value)
      store.commit("alterCurrentMulitLineClass", currentMultiClass.value)

      const combinedUrl2 = `/line_chart/getcolumns?table_name=${store.state.controlParams.currentMultiLineClass}`;
      console.log("combinedUrl2", combinedUrl2);
      const data2 = get(combinedUrl2);
      data2.then(res => {
          console.log("getcolumns", res['columns']);
          const columns = res['columns'];
          const array = ref([...columns])
          console.log("columns:", array);
          store.commit("ColumnsChange", array);
          currentMultiClassLines.value = res['columns'];
      });
      
    }

    const handleColumnsChange = () => {
      console.log("handleColumnsChange",currentMultiClassLines.value)
      // if(currentMultiClassLines.value == 'all'){
      //   const newValue = multiLineClassAndLinesMap.get('columns')
      //   store.commit("ColumnsChange", newValue)
      // }
      // else
      store.commit("ColumnsChange", currentMultiClassLines.value)
      // if(currentMultiClassLines.value == 'all'){
      //   store.commit("ColumnsChange", state.allMultiLineClassAndLinesMap);
      // }
    }

    const handleMultiLineClassALineChange = () => {
      store.commit("alterCurrentMulitLineClassALine", currentMultiClassALine.value)
    }
    const handleMultiLineClassLinesChange = () => {

      console.log("handleMultiLineClassLinesChange",currentMultiClassLines.value)
      store.commit("alterCurrentMulitLineClassLines", currentMultiClassLines.value)
    }

    const handleTableChange = () => {
      store.commit("alterTable", currentTable);
    };

    const handleCustomTableChange = () => {
      store.commit("alterCustomTable", currentCustomTable.value)
    }

    const loadCustomTableAndInfo = () => {
      store.dispatch("loadCustomTableAndInfo");
    }

    const handleProgressiveChange = () => {
      store.commit("alterProgressive", progressive.value);
    }

    const clearIndexFlag=()=>{
      clearFlagDB()
    }



    return {
      allAlgoritem,
      chooseMode,
      chooseLineType,
      handleModeChange,
      currentSampleAlgorithm,
      allSampleAlgoritem,
      handleSampleMethodChange,
      currentTable,
      allTables,
      allDefaultTables,
      handleTableChange,
      handleLineTypeChange,
      allMultLineClass,
      currentMultiClass,
      currentMultiClassALine,
      currentMultiClassLines,
      handleMultiLineClassChange,
      handleColumnsChange,
      handleAddAverage,
      handleMultiLineClassALineChange,
      dialogFormVisible,
      handleCustomTableChange,
      loadCustomTableAndInfo,
      allCustomTables,
      allCustomMultiLineClass,
      multiLineClassAndLinesMap,
      multiLineTransformDialogVisible,
      progressive,
      handleProgressiveChange,
      clearIndexFlag,
      handleComputePanel,
      handleExperiment,
      selectedOption,
      handleSelectedOption,
      selectedExperiment,
      handleSelectedExperiment,
      selectedAggregate,
      handleSelectedAggregate,
      selectedComputeOrShow,
      handleSelectedComputeOrShow,
      widthRef,
      heightRef,
      errorBound
    };
  },
});
</script>
<style scoped>
.data-control-panel {
  min-width: 200px;
  max-width: 210px;
  flex-grow: 0.15;
}

.choose_container {
  margin-top: 10px;
}

.btn {
  width: 100%;
  margin-left: 0px;
}

.multi-line-select {
  width: 100%;
}

.device-control-panel {
  min-height: 25px;
  background-color: #fff;
}

.device-dimensions-menu-show {
  position: absolute;
  inset: 0px auto auto 0px;
  margin: 0px;
  transform: translate(0px, 40px);
}

.dim-input {
  max-width: 4rem;
}
</style>