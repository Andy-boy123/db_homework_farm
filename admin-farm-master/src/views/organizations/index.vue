<template>
  <div class="app-container">
    <div class="farm-talbe-header" style="padding-bottom: 20px">
      <el-button type="primary" @click="handleClickAddBtn">添加组织</el-button>
    </div>
    <el-table :data="tableData" v-loading="listLoading" border style="width: 100%">
      
      <!-- <el-table-column prop="organization_id" label="主键"> </el-table-column> -->
      <el-table-column prop="organization_name" label="单位名称"> </el-table-column>
      <el-table-column prop="organization_type" label="单位类型"></el-table-column>
      <el-table-column prop="additional_info" label="地址"></el-table-column>
      
      <el-table-column fixed="right" label="操作" width="100">
        <template slot-scope="scope">
          <el-button type="danger" @click="remove(scope.row)">删除</el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-dialog title="添加组织" :visible.sync="dialogFormVisible">
      <el-form ref="ruleForm" :model="form" :rules="rules">
        <el-form-item label="单位名称" prop="name">
          <el-input v-model="form.name" placeholder="请填写单位名称" />
        </el-form-item>
        <el-form-item label="单位类型" prop="type">
          <el-input v-model="form.type" placeholder="请填写单位类型" />
        </el-form-item>
        <el-form-item label="单位地址" prop="info">
          <el-input v-model="form.info" placeholder="请填写单位地址" />
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
import { getOrganizations, addOrganizations, deleteOrganizations } from '@/api/organizations'

export default {
    name: 'Organizations',
  data() {
    return {
      tableData: [],
      dialogFormVisible: false,
      form: {
        "name": "",
        "type": "",
        "info": ""
      },
      listLoading: true,
      addLoading: false,
      rules: {
        name: [
          { required: true, message: "请填写单位名称", trigger: "blur" },
        ],
        type: [
          { required: true, message: "请填写单位类型", trigger: "blur" },
        ],
        info: [
          { required: true, message: "请填写单位地址", trigger: "blur" },
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
      getOrganizations().then((res) => {
        this.tableData = res;
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
        const organization_name = row.organization_name
        deleteOrganizations({
          organization_name
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
    handleClickAddBtn() {
      this.form = {
        "name": "",
        "type": "",
        "info": ""
      }
      this.dialogFormVisible = true
    },
    handleAdd() {
      this.$refs.ruleForm.validate((valid) => {
        if (valid) {
          this.addLoading = true
          addOrganizations(this.form)
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
