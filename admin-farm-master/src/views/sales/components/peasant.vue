<template>
    <div class="app-container">
        <div class="farm-talbe-header" style="padding-bottom: 20px">
            <el-button type="primary" icon="el-icon-circle-plus-outline" @click="handleClickAddBtn">添加销售信息</el-button>
        </div>
        <el-table :data="tableData" v-loading="listLoading" border style="width: 100%">
            <el-table-column prop="begin_time" label="上架时间">
                <template slot-scope="scope">
                    {{ formatTime(scope.row, 'begin_time') }}
                </template>
            </el-table-column>


            <el-table-column prop="buyer_name" label="买家"> </el-table-column>
            <el-table-column prop="crop_name" label="作物名称"> </el-table-column>
            <el-table-column prop="quantity" label="数量"> </el-table-column>
            <el-table-column prop="sale" label="总售价"> </el-table-column>

            <el-table-column prop="sale_time" label="销售时间">
                <template slot-scope="scope">
                    {{ formatTime(scope.row, 'sale_time') }}
                </template>
            </el-table-column>


            <el-table-column prop="sale_type" label="销售状态">
                <template slot-scope="scope">
                    <span v-if="scope.row.sale_type === 0">待售</span>
                    <span v-if="scope.row.sale_type === 1">售出</span>
                </template>
            </el-table-column>


            <el-table-column fixed="right" label="操作" width="150">
                <template slot-scope="scope">
                    <!-- <el-button type="primary" icon="el-icon-edit" @click="handleClickModifyRow(scope.row)">编辑</el-button> -->
                    <el-button v-if="scope.row.sale_time == null" type="danger" icon="el-icon-delete" @click="handleRemoveRow(scope.row)">删除</el-button>
                </template>
            </el-table-column>
        </el-table>

        <el-dialog title="添加销售信息" :visible.sync="addVisible">
            <el-form ref="addForm" :model="addForm" :rules="addRules" class="custom-add-form custom-add-form--label-100">
                <el-form-item label="作物类型" prop="crop_type_id">
                    <el-select v-model="addForm.crop_type_id" placeholder="请选择作物类型">
                        <el-option v-for="item in crop_types" :key="item.crop_type_id" :label="item.crop_name"
                            :value="item.crop_type_id">
                        </el-option>
                    </el-select>
                </el-form-item>

                <el-form-item label="数量" prop="quantity">
                    <el-input-number v-model="addForm.quantity" :precision="2"></el-input-number>
                </el-form-item>

                <el-form-item label="售价" prop="sale">
                    <el-input-number v-model="addForm.sale" :precision="2"></el-input-number>
                </el-form-item>
             

            </el-form>

            <div slot="footer" class="dialog-footer">
                <el-button @click="addVisible = false">取 消</el-button>
                <el-button type="primary" @click="handleAdd" :loading="addLoading">确 定</el-button>
            </div>
        </el-dialog>

    </div>
</template>
<script>
import moment from "moment";
import { getCropTypes } from "@/api/crops";
import { getAllUsers } from "@/api/user";
import {
    getMassifs,
    addMassifs,
    updateMassifs,
    deleteMassifs,
} from "@/api/massifs";

import { getPersonalSales, addSale, deleteSale } from "@/api/sales";

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
                crop_type_id: undefined,
                quantity: undefined,
                sale: undefined,
            },
            addRules: {
                crop_type_id: [
                    { required: true, message: "请选择作物类型", trigger: "blur" },
                ],
                quantity: [
                    { required: true, message: "请填写数量", trigger: "blur" },
                ],
                sale: [
                    { required: true, message: "请填写售价", trigger: "blur" },
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
        //   getAllUsers().then((res) => {
        //     this.users = res;
        //   });

        this.getList();
    },
    methods: {
        formatTime(row, key) {
            if (!row[key]) {
                return ''
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
            getPersonalSales().then((res) => {
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
                    const sale_id = row.sale_id;
                    deleteSale({
                        sale_id,
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
                    addSale(this.addForm)
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
    },
};
</script>
  