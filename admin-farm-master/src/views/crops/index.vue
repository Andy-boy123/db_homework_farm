<template>
  <div class="app-container">
    <div class="farm-talbe-header" style="padding-bottom: 20px">
      <el-button type="primary" @click="dialogFormVisible = true">添加作物</el-button>
    </div>
    <el-table :data="tableData" v-loading="listLoading" border style="width: 100%">
      <el-table-column prop="crop_name" label="作物名称"> </el-table-column>
      <el-table-column prop="crop_type_id" label="crop_type_id">
      </el-table-column>
      <el-table-column fixed="right" label="操作" width="100">
        <template slot-scope="scope">
          <el-button type="danger" @click="remove(scope.row)">删除</el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-dialog title="添加作物" :visible.sync="dialogFormVisible">
      <el-form ref="ruleForm" :model="form" :rules="rules">
        <el-form-item label="作物名称" prop="crop_name">
          <el-input v-model="form.crop_name" placeholder="请填写作物名称" />
        </el-form-item>
      </el-form>

      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogFormVisible = false">取 消</el-button>
        <el-button type="primary" @click="handleAdd" :loading="addLoading">确 定</el-button>
      </div>
    </el-dialog>
  </div>
</template>
<script>
import { getCropTypes, addCropType, deleteCropType } from "@/api/crops";

export default {
  name: "Crops",
  data() {
    return {
      tableData: [],
      dialogFormVisible: false,
      listLoading: true,
      addLoading: false,
      form: {
        crop_name: "",
      },
      rules: {
        crop_name: [
          { required: true, message: "请填写作物名称", trigger: "blur" },
        ],
      },
    };
  },
  created() {
    this.getList();
  },
  methods: {
    getList() {
      this.listLoading = true
      getCropTypes().then((res) => {
        const { crop_types } = res;
        this.tableData = crop_types;
      })
      .finally(() => {
        this.listLoading = false
      })
    },
    remove(row) {
      this.$confirm('此操作将删除该行数据, 是否继续?', '提示', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        const crop_type_id = row.crop_type_id
        deleteCropType({
          crop_type_id
        })
          .then((res) => {
            this.$message({
              type: 'success',
              message: '删除成功!'
            });
          })
          .finally(() => {
            this.getList();
          });


      }).catch(() => {
        this.$message({
          type: 'info',
          message: '已取消删除'
        });
      });
    },
    handleAdd() {
      this.$refs.ruleForm.validate((valid) => {
        if (valid) {
          this.addLoading = true
          addCropType(this.form)
            .then((res) => {
              this.$message.success("添加成功");
              this.dialogFormVisible = false;
            })
            .finally(() => {
              this.addLoading = false
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
