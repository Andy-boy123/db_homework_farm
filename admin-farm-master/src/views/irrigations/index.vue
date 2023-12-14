<template>
  <div class="app-container">
    <div class="farm-talbe-header" style="padding-bottom: 20px">
      <el-button type="primary" icon="el-icon-circle-plus-outline" @click="addVisible = true">添加灌溉记录</el-button>
    </div>
    <el-table :data="tableData" v-loading="listLoading" border style="width: 100%">
      <!-- <el-table-column prop="irrigation_id" label="主键"> </el-table-column> -->
      <el-table-column prop="massif_id" label="地块编号"> </el-table-column>
      <el-table-column prop="crop_name" label="作物"> </el-table-column>
      <el-table-column prop="irrigate_quantity" label="灌溉量"> </el-table-column>
      <el-table-column prop="irrigate_time" label="灌溉时间"> </el-table-column>
      <el-table-column prop="username" label="username">
      </el-table-column>
      <el-table-column fixed="right" label="操作" width="220">
        <template slot-scope="scope">
          <el-button type="danger" icon="el-icon-delete" @click="handleRemoveRow(scope.row)">删除</el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-dialog title="添加收获记录" :visible.sync="addVisible">
      <el-form ref="addForm" :model="addForm" :rules="addRules" class="custom-add-form custom-add-form--label-100">
        <el-form-item label="地块编号" prop="massif_id">
          <el-select v-model="addForm.massif_id" placeholder="请选择地块编号">
            <el-option v-for="item in massifsOptions" :key="item.massif_id" :label="item.massif_id"
              :value="item.massif_id">
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="灌溉量" prop="irrigate_quantity">
          <el-input-number v-model="addForm.irrigate_quantity" :precision="2"></el-input-number>
        </el-form-item>
        <el-form-item label="灌溉时间" prop="irrigate_time">
          <el-date-picker v-model="addForm.irrigate_time" type="datetime" placeholder="选择日期时间">
          </el-date-picker>
        </el-form-item>
      </el-form>

      <div slot="footer" class="dialog-footer">
        <el-button @click="addVisible = false">取 消</el-button>
        <el-button type="primary" @click="handleAdd" :loading="addLoading">确 定</el-button>
      </div>
    </el-dialog>

    <el-dialog title="修改地块" :visible.sync="editVisible">
      <el-form ref="addForm" :model="addForm" :rules="addRules" class="custom-add-form custom-add-form--label-100">
        <!-- <el-form-item label="作物类型" prop="crop_type_id">
          <el-select v-model="addForm.crop_type_id" placeholder="请选择作物类型">
            <el-option v-for="item in crop_types" :key="item.crop_type_id" :label="item.crop_name"
              :value="item.crop_type_id">
            </el-option>
          </el-select>
        </el-form-item> -->
        <el-form-item label="播种时间" prop="new_sowing_time">
          <el-date-picker v-model="addForm.new_sowing_time" type="datetime" placeholder="选择日期时间">
          </el-date-picker>
        </el-form-item>
        <el-form-item label="预计收获时间" prop="new_ex_harvest_time">
          <el-date-picker v-model="addForm.new_ex_harvest_time" type="datetime" placeholder="选择日期时间">
          </el-date-picker>
        </el-form-item>
        <el-form-item label="地块所有者" prop="new_crop_type_id">
          <el-select v-model="addForm.new_crop_type_id" placeholder="请选择地块所有者">
            <el-option v-for="item in users" :key="item.user_id" :label="item.username" :value="item.user_id">
            </el-option>
          </el-select>
        </el-form-item>
      </el-form>

      <div slot="footer" class="dialog-footer">
        <el-button @click="editVisible = false">取 消</el-button>
        <el-button type="primary" @click="handleAdd" :loading="addLoading">确 定</el-button>
      </div>
    </el-dialog>
  </div>
</template>
<script>
import { getCropTypes } from "@/api/crops";
import { getAllUsers } from "@/api/user";
import { getHarvest, addHarvest, deleteHarvest } from "@/api/harvests";


import {
  getMassifs,
} from "@/api/massifs";

import {
  getIrrigation,
  addIrrigation,
  deleteIrrigation,
} from "@/api/irrigations";


export default {
    name: 'Irrigations',
  data() {
    return {
      listLoading: true,
      tableData: [],
      form: {
        crop_name: "",
      },
      rules: {
        crop_name: [
          { required: true, message: "请填写作物名称", trigger: "blur" },
        ],
      },
      crop_types: [],
      users: [],
      massifsOptions: [],

      addVisible: false,
      addLoading: false,
      addForm: {
        "massif_id": undefined,
        "irrigate_quantity": undefined,
        "irrigate_time": undefined,

      },
      addRules: {
        massif_id: [
          { required: true, message: "请选择地块编号", trigger: "blur" },
        ],
        irrigate_quantity: [
          { required: true, message: "请填写灌溉量", trigger: "blur" },
        ],
        irrigate_time: [
          { required: true, message: "请选择灌溉时间", trigger: "blur" },
        ],
      },

      editVisible: false,
      editLoading: false,
      editForm: {
        massif_id: undefined,
        new_crop_type_id: undefined,
        new_sowing_time: undefined,
        new_ex_harvest_time: undefined,
      },
    };
  },
  created() {
    getCropTypes().then((res) => {
      const { crop_types } = res;
      this.crop_types = crop_types;
    });
    getAllUsers().then((res) => {
      this.users = res;
    });

    getMassifs().then((res) => {
      const { massifs } = res;
      this.massifsOptions = massifs;
    });

    this.getList();
  },
  methods: {
    getList() {
      this.listLoading = true
      getIrrigation().then((res) => {
        const { irrigations } = res;
        this.tableData = irrigations;
      })
      .finally(() => {
      this.listLoading = false
      })
    },
    handleRemoveRow(row) {
      this.$confirm("此操作将删除该行数据, 是否继续?", "提示", {
        confirmButtonText: "确定",
        cancelButtonText: "取消",
        type: "warning",
      })
        .then(() => {
          const irrigation_id = row.irrigation_id;
          deleteIrrigation({
            irrigation_id,
          })
            .then((res) => {
              this.$message({
                type: "success",
                message: "删除成功!",
              });
            })
            .finally(() => {
              this.getList();
            });
        })
        .catch(() => {
          this.$message({
            type: "info",
            message: "已取消删除",
          });
        });
    },
    handleAdd() {
      this.$refs.addForm.validate((valid) => {
        if (valid) {
          this.addLoading = true;
          addIrrigation(this.addForm)
            .then((res) => {
              this.$message.success("添加成功");
              this.addVisible = false;
            })
            .finally(() => {
              this.addLoading = false;
              this.getList();
            });
        } else {
          console.log("error submit!!");
          return false;
        }
      });
    },
  },
};
</script>
