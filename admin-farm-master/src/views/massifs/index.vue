<template>
  <div class="app-container">
    <div class="farm-talbe-header" style="padding-bottom: 20px">
      <el-button type="primary" icon="el-icon-circle-plus-outline" @click="handleClickAddBtn">添加地块</el-button>
    </div>
    <el-table :data="tableData" v-loading="listLoading" border style="width: 100%">
      <el-table-column prop="massif_id" label="地块编号"> </el-table-column>
      <el-table-column prop="crop_type" label="作物类型"> </el-table-column>
      <el-table-column prop="planting_date" label="播种时间"> </el-table-column>
      <el-table-column prop="harvest_date" label="预计收获时间">
      </el-table-column>

      <el-table-column fixed="right" label="操作" width="220">
        <template slot-scope="scope">
          <el-button type="primary" icon="el-icon-edit" @click="handleClickModifyRow(scope.row)">编辑</el-button>
          <el-button type="danger" icon="el-icon-delete" @click="handleRemoveRow(scope.row)">删除</el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-dialog title="添加地块" :visible.sync="addVisible">
      <el-form ref="addForm" :model="addForm" :rules="addRules" class="custom-add-form custom-add-form--label-100">
        <el-form-item label="作物类型" prop="crop_type_id">
          <el-select v-model="addForm.crop_type_id" placeholder="请选择作物类型">
            <el-option v-for="item in crop_types" :key="item.crop_type_id" :label="item.crop_name"
              :value="item.crop_type_id">
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="播种时间" prop="sowing_time">
          <el-date-picker v-model="addForm.sowing_time" type="datetime" placeholder="选择日期时间">
          </el-date-picker>
        </el-form-item>
        <el-form-item label="预计收获时间" prop="ex_harvest_time">
          <el-date-picker v-model="addForm.ex_harvest_time" type="datetime" placeholder="选择日期时间">
          </el-date-picker>
        </el-form-item>
        <el-form-item label="地块所有者" prop="massif_owner_id">
          <el-select v-model="addForm.massif_owner_id" placeholder="请选择地块所有者">
            <el-option v-for="item in users" :key="item.user_id" :label="item.username" :value="item.user_id">
            </el-option>
          </el-select>
        </el-form-item>
      </el-form>

      <div slot="footer" class="dialog-footer">
        <el-button @click="addVisible = false">取 消</el-button>
        <el-button type="primary" @click="handleAdd" :loading="addLoading">确 定</el-button>
      </div>
    </el-dialog>

    <el-dialog title="修改地块" :visible.sync="editVisible">
      <el-form ref="editForm" :model="editForm" :rules="editRules" class="custom-add-form custom-add-form--label-100">
        <el-form-item label="作物类型" prop="new_crop_type_id">
          <el-select v-model="editForm.new_crop_type_id" placeholder="请选择作物类型">
            <el-option v-for="item in crop_types" :key="item.crop_type_id" :label="item.crop_name"
              :value="item.crop_type_id">
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="播种时间" prop="new_sowing_time">
          <el-date-picker v-model="editForm.new_sowing_time" type="datetime" placeholder="选择日期时间">
          </el-date-picker>
        </el-form-item>
        <el-form-item label="预计收获时间" prop="new_ex_harvest_time">
          <el-date-picker v-model="editForm.new_ex_harvest_time" type="datetime" placeholder="选择日期时间">
          </el-date-picker>
        </el-form-item>
      </el-form>

      <div slot="footer" class="dialog-footer">
        <el-button @click="editVisible = false">取 消</el-button>
        <el-button type="primary" @click="handleEdit" :loading="editLoading">确 定</el-button>
      </div>
    </el-dialog>
  </div>
</template>
<script>
import { getCropTypes } from "@/api/crops";
import { getAllUsers } from "@/api/user";
import {
  getMassifs,
  addMassifs,
  updateMassifs,
  deleteMassifs,
} from "@/api/massifs";

export default {
  name: "Crops",
  data() {
    return {
      tableData: [],
      listLoading: true,

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

      addVisible: false,
      addLoading: false,
      addForm: {
        crop_type_id: undefined, // 作物id
        sowing_time: undefined,  // 播种时间
        ex_harvest_time: undefined,  // 预计收获时间
        massif_owner_id: undefined, // 地块所有者
      },
      addRules: {
        crop_type_id: [
          { required: true, message: "请选择作物类型", trigger: "blur" },
        ],
        sowing_time: [
          { required: true, message: "请选择播种时间", trigger: "blur" },
        ],
        ex_harvest_time: [
          { required: true, message: "请选择预计收获时间", trigger: "blur" },
        ],
        massif_owner_id: [
          { required: true, message: "请选择地块所有者", trigger: "blur" },
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
      editRules: {
        new_crop_type_id: [
          { required: true, message: "请选择作物类型", trigger: "blur" },
        ],
        new_crop_type_id: [
          { required: true, message: "请选择播种时间", trigger: "blur" },
        ],
        new_ex_harvest_time: [
          { required: true, message: "请选择预计收获时间", trigger: "blur" },
        ],
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

    this.getList();
  },
  methods: {
    getList() {
      this.listLoading = true
      getMassifs().then((res) => {
        const { massifs } = res;
        this.tableData = massifs;
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
          const massif_id = row.massif_id;
          deleteMassifs({
            massif_id,
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
          addMassifs(this.addForm)
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
    handleClickAddBtn() {
      this.addForm = {
        crop_type_id: undefined, // 作物id
        sowing_time: undefined,  // 播种时间
        ex_harvest_time: undefined,  // 预计收获时间
        massif_owner_id: undefined, // 地块所有者
      },
      this.addVisible = true
    },
    handleClickModifyRow(row) {
 
      this.editForm = {
        massif_id: undefined,
        new_crop_type_id: undefined,
        new_sowing_time: undefined,
        new_ex_harvest_time: undefined,
      }
      this.editForm = {
        massif_id: row.massif_id,
        new_crop_type_id: row.crop_type_id,
        new_sowing_time: row.planting_date,
        new_ex_harvest_time: row.harvest_date,
      }
      
      this.editVisible = true;
    },
    handleEdit() {
      this.editVisible = false
      this.$refs.editForm.validate((valid) => {
        if (valid) {
          this.editLoading = true;
          updateMassifs(this.editForm)
            .then((res) => {
              this.$message.success("修改成功");
              this.addVisible = false;
            })
            .finally(() => {
              this.editLoading = false;
              this.editVisible = false
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
