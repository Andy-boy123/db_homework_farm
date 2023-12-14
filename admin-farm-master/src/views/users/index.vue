<template>
  <div class="app-container">
    <el-table :data="tableData" v-loading="listLoading" border style="width: 100%">
      <!-- <el-table-column prop="user_id" label="主键"> </el-table-column> -->
      <el-table-column prop="username" label="用户名"> </el-table-column>
      <!-- <el-table-column prop="password" label="密码"> </el-table-column> -->
      
      <el-table-column prop="phonenumber" label="手机号"> </el-table-column>
      <el-table-column prop="organization_id" label="用户单位编号">
        <template slot-scope="scope">
          {{ organizationDict[scope.row.organization_id] }}
        </template>
      </el-table-column>
      <el-table-column prop="useremail" label="邮箱地址"> </el-table-column>
      <el-table-column prop="user_online" label="用户在线状态">
        <template slot-scope="scope">
          {{ online[scope.row.user_online] }}
        </template>
      </el-table-column>
      <el-table-column prop="user_lastonline" label="上次登陆时间">
        <template slot-scope="scope">
          {{ formatTime(scope.row) }}
        </template>
      </el-table-column>
      <el-table-column prop="user_type" label="用户类型">
        <template slot-scope="scope">
          {{ userTypes[scope.row.user_type] }}
        </template>
      </el-table-column>

      <el-table-column fixed="right" label="操作" width="120">
        <template slot-scope="scope">
          <el-button type="primary" icon="el-icon-edit" @click="handleClickModifyRow(scope.row)">编辑</el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-dialog title="修改地块" :visible.sync="editVisible">
      <el-form ref="editForm" :model="editForm" :rules="editRules" class="custom-add-form custom-add-form--label-100">
        <el-form-item label="姓名" prop="username">
          <el-input v-model="editForm.username" />
        </el-form-item>
        <el-form-item label="手机" prop="phonenumber" >
          <el-input v-model="editForm.phonenumber" />
        </el-form-item>
        <el-form-item label="密码" prop="password">
          <el-input v-model="editForm.password" />
        </el-form-item>
        <el-form-item label="邮箱" prop="useremail">
          <el-input v-model="editForm.useremail" />
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
import moment from "moment";
import { getAllUsers, updateUserInfo } from "@/api/user";
import { getOrganizations } from '@/api/organizations'

export default {
  name: "Users",
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
        massif_id: undefined,
        irrigate_quantity: undefined,
        irrigate_time: undefined,
      },
      editRules: {
        username: [
          { required: true, message: "请填写姓名", trigger: "blur" },
        ],
        phonenumber: [
          { required: true, message: "请填写手机", trigger: "blur" },
        ],
        password: [
          { required: true, message: "请填写密码", trigger: "blur" },
        ],
        useremail: [
          { required: true, message: "请填写邮箱", trigger: "blur" },
        ],
      },

      editVisible: false,
      editLoading: false,
      editForm: {
        username: undefined,
        phonenumber: undefined,
        useremail: undefined,
        password: undefined,
      },

      userTypes: {
        '0': '农户',
        '1': '粮企',
        '2': '政府用户',
        '999': '管理员',
      },
      online: {
        '0': '离线',
        '1': '在线',
      },
      organizationDict: {}
    };
  },
  created() {
    getOrganizations()
      .then(res => {
        // this.organizations = res
        const organizationDict = res.reduce((obj, item) => {
          obj[item.organization_id] = item.organization_name;
          return obj;
        }, {});
        this.organizationDict = organizationDict

      })
      .catch(err => {
        console.log(err)
      })
      .finally(() => {
        // this.getList();

      })
    this.getList();


  },
  methods: {
    formatTime(row) {
      const originalDateString = row.user_lastonline;
      const momentDate = moment(
        originalDateString,
        "ddd, DD MMM YYYY HH:mm:ss [GMT]"
      );

      const formattedDateString = momentDate.format("YYYY-MM-D HH:mm:ss");
      return formattedDateString;
    },

    getList() {
      this.listLoading = true
      getAllUsers().then((res) => {
        this.tableData = res;
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
    handleEdit() {
      this.$refs.editForm.validate((valid) => {
        if (valid) {
          this.editLoading = true;
          updateUserInfo(this.editForm)
            .then((res) => {
              this.$message.success("修改成功");
              this.editVisible = false;
            })
            .finally(() => {
              this.editLoading = false;
              this.getList();
            });
        } else {
          console.log("error submit!!");
          return false;
        }
      });
    },
    handleClickModifyRow(row) {
      const {username,phonenumber,useremail,password} = row
      this.$set(this, 'editForm', {username,phonenumber,useremail,password})
      this.editVisible = true;
    },

  },
};
</script>
