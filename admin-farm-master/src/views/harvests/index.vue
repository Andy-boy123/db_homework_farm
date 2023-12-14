<template>
  <div class="app-container">
    <div class="farm-talbe-header" style="padding-bottom: 20px">
      <el-button type="primary" icon="el-icon-circle-plus-outline" @click="addVisible = true">添加收获记录</el-button>
    </div>
    <el-table :data="tableData" v-loading="listLoading" border style="width: 100%">
      <!-- <el-table-column prop="harvest_id" label="主键"> </el-table-column> -->
      <el-table-column prop="harvest_quantity" label="收获数量"> </el-table-column>
      <el-table-column prop="sowing_time" label="播种时间"> </el-table-column>
      <el-table-column prop="harvest_time" label="收获时间">
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
        <el-form-item label="收获数量" prop="harvest_quantity">
          <el-input-number v-model="addForm.harvest_quantity" :precision="2"></el-input-number>
        </el-form-item>
        <el-form-item label="播种时间" prop="sowing_time">
          <el-date-picker v-model="addForm.sowing_time" type="datetime" placeholder="选择日期时间">
          </el-date-picker>
        </el-form-item>
        <el-form-item label="预计收获时间" prop="harvest_time">
          <el-date-picker v-model="addForm.harvest_time" type="datetime" placeholder="选择日期时间">
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
  addMassifs,
  updateMassifs,
  deleteMassifs,
} from "@/api/massifs";

export default {
  name: 'Harvests',
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
        "harvest_quantity": undefined,
        "harvest_time": undefined,
        "sowing_time": undefined,

      },
      addRules: {
        massif_id: [
          { required: true, message: "请选择地块编号", trigger: "blur" },
        ],
        harvest_quantity: [
          { required: true, message: "请填写收获数量", trigger: "blur" },
        ],
        sowing_time: [
          { required: true, message: "请选择播种时间", trigger: "blur" },
        ],
        harvest_time: [
          { required: true, message: "请选择收获时间", trigger: "blur" },
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
      getHarvest().then((res) => {
        const { harvests } = res;
        this.tableData = harvests;
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
          const harvest_id = row.harvest_id;
          deleteHarvest({
            harvest_id,
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
          addHarvest(this.addForm)
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
