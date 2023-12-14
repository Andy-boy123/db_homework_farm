<template>
    <div class="app-container">
        <!-- <div class="farm-talbe-header" style="padding-bottom: 20px">
            <el-button type="primary" @click="dialogFormVisible = true">添加作物</el-button>
        </div> -->
        <el-table :data="tableData" v-loading="listLoading" border style="width: 100%">
            <el-table-column prop="begin_time" label="销售时间">
                <template slot-scope="scope">
                    {{ formatTime(scope.row, 'begin_time') }}
                </template>
            </el-table-column>
            <el-table-column prop="crop_name" label="作物名称"> </el-table-column>
            <el-table-column prop="quantity" label="数量"> </el-table-column>
            <el-table-column prop="sale" label="总售价"> </el-table-column>
            <el-table-column prop="seller_username" label="销售方"> </el-table-column>

            <el-table-column fixed="right" label="操作" width="180">
                <template slot-scope="scope">
                    <el-button type="primary" @click="handleClickRow(scope.row)">添加购买记录</el-button>
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
import moment from "moment";
import { getCropTypes, addCropType, deleteCropType } from "@/api/crops";
import { getViewSalesMarket, addPurchase } from "@/api/sales";


export default {
    name: 'Admin',
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
        formatTime(row, key) {
            if (!row[key]) {
                return '';
            }
            const originalDateString = row[key];
            const momentDate = moment(
                originalDateString,
                "ddd, DD MMM YYYY HH:mm:ss [GMT]"
            );

            const formattedDateString = momentDate.format("YYYY-MM-D HH:mm:ss");
            return formattedDateString;
        },
        getList() {
            this.listLoading = true
            getViewSalesMarket().then((res) => {
                this.tableData = res;
            })
                .finally(() => {
                    this.listLoading = false
                })
        },
        handleClickRow(row) {
            const sale_id = row.sale_id
            addPurchase({
                sale_id
            })
                .then((res) => {
                    this.$message({
                        type: 'success',
                        message: '添加成功!'
                    });
                })
                .finally(() => {
                    this.getList();
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
  