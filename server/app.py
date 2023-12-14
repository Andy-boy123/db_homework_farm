from flask import Flask
from flask_cors import CORS
import jwt
from flask import jsonify, request
from db import *
from functools import wraps
import psycopg2.extras

app = Flask(__name__)
CORS(app)
app.config['SECRET_KEY'] = '0f574d86d10ae8778feffc0dc47810907e436a8fc14c2971'


# 装饰器，用于验证JWT token
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None

        if 'Authorization' in request.headers:
            token = request.headers['Authorization']
            if token.startswith('Bearer '):
                token = token[len('Bearer '):]  # 去掉前缀"Bearer "

        if not token:
            return jsonify({'message': 'Token is missing!'}), 401

        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            current_user_id = data['user_id']
        except:
            return jsonify({'message': 'Token is invalid!'}), 401

        # 检查用户在线状态
        user_online = check_user_online_status(current_user_id)
        if not user_online:
            return jsonify({'message': 'Token is invalid or user is offline!'}), 401

        return f(current_user_id, *args, **kwargs)

    return decorated


@app.route('/list_tables')
def show_tables():
    tables = list_tables()
    return jsonify(tables)


@app.route('/register', methods=['POST'])
def register():
    data = request.json
    username = data.get('username')
    phonenumber = data.get('phonenumber')
    organization = data.get('organization')
    useremail = data.get('useremail')
    password = data.get('password')  # 应考虑安全地处理密码
    user_type = data.get('user_type')

    try:
        result = register_user(username, phonenumber, organization, useremail, password, user_type)

        if isinstance(result, tuple) and result[1] == 400:
            return jsonify({'error': result[0]}), result[1]

        # 用户注册成功...
        return jsonify({'message': 'User registered successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@app.route('/login', methods=['POST'])
def login():
    data = request.json
    login = data.get('login')  # 用户名、手机号或邮箱
    password = data.get('password')

    user = authenticate_user(login, password)
    if user:
        # 生成token，有效期为24小时
        token = jwt.encode({
            'user_id': user[0],  # user_id
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24)
        }, app.config['SECRET_KEY'], algorithm='HS256')

        return jsonify({
            'message': 'Login successful',
            'username': user[1],  # 用户名
            'phonenumber': user[2],  # 手机号
            'email': user[4],  # 邮箱号
            'token': token,
            'last_login': user[7],  # 上次登录时间
            'user_type': user[8],  # 用户类型
            'organization_name': user[9],  # 组织名
            'organization_type': user[10]  # 用户组
        })

    return jsonify({'message': 'Invalid credentials'}), 401


@app.route('/logout', methods=['POST'])
@token_required
def logout(current_user_id):
    user = logout_user(current_user_id)

    if user is None:
        return jsonify({'message': 'User not found'}), 401
    elif user == 'Already logged out':
        return jsonify({'message': 'User already logged out'}), 200
    else:
        return jsonify({'message': 'Logout successful'})


@app.route('/update_user_info', methods=['POST'])
@token_required
def user_info_update(current_user_id):
    data = request.json

    # 提取要更新的用户信息字段
    new_username = data.get('username')
    new_phonenumber = data.get('phonenumber')
    new_useremail = data.get('useremail')
    new_password = data.get('password')

    # 根据用户名在users表中查询user_id
    user_id = get_user_id_by_username(new_username)
    if user_id is None:
        return jsonify({'error': 'User not found'}), 404

    # 检查权限：只有管理员或当前用户可以修改用户信息
    if current_user_id != user_id and not is_admin(current_user_id):
        return jsonify({'error': 'Unauthorized to update user info'}), 401

    result = update_user_info(user_id, new_username, new_phonenumber, new_useremail, new_password)
    if isinstance(result, tuple):
        return jsonify({'error': result[0]}), result[1]

    return jsonify({'message': result})


@app.route('/add_organization', methods=['POST'])
@token_required
def organization_management(user_id):
    data = request.json
    action = 'add'  # 'add', 'update', or 'delete'
    organization_id = data.get('organization_id')  # For update and delete actions
    name = data.get('name')  # For add and update actions
    type = data.get('type')  # For add and update actions
    info = data.get('info')  # For add and update actions

    # Check if user is an admin
    if not is_admin(user_id):
        return jsonify({'error': 'Unauthorized access'}), 401

    result = manage_organization(action, organization_id, name, type, info)
    if isinstance(result, tuple):
        return jsonify({'error': result[0]}), result[1]

    return jsonify({'message': result})


@app.route('/delete_organization', methods=['POST'])
@token_required
def delete_organization(user_id):
    data = request.json
    organization_name = data.get('organization_name')
    # Check if user is an admin
    if not is_admin(user_id):
        return jsonify({'error': 'Unauthorized access'}), 401

    if not organization_name:
        return jsonify({'error': 'Organization name is required'}), 400

    result = delete_organization_record(organization_name)
    if isinstance(result, tuple):
        return jsonify({'error': result[0]}), result[1]

    return jsonify({'message': 'Organization deleted successfully'})


@app.route('/get_organizations', methods=['GET'])
# @token_required
def get_organizations():
    # 验证用户类型
    # if not is_authorized_user(user_id, [999, 2]):
    #    return jsonify({'error': 'Unauthorized access'}), 401

    # 获取组织信息
    organizations = fetch_organizations()
    if isinstance(organizations, tuple):
        return jsonify({'error': organizations[0]}), organizations[1]

    return jsonify(organizations)


@app.route('/farmer_massifs', methods=['GET'])
@token_required
def farmer_massifs(user_id):
    current_user_id = user_id

    # 检查用户权限
    if not is_authorized_user(current_user_id, [0]):  # 假设0是农户用户类型
        return jsonify({'error': 'Unauthorized access'}), 401

    try:
        data = get_farmer_massifs_view_data(current_user_id)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/add_crop_type', methods=['POST'])
@token_required
def add_crop_type(user_id):
    data = request.json
    crop_name = data.get('crop_name')

    # 验证用户是否具有管理员权限
    if not is_authorized_user(user_id, [999]):
        return jsonify({'error': 'Unauthorized access'}), 403

    # 调用 db.py 中的函数进行数据库操作
    message, status = add_crop_type_db(crop_name)
    return jsonify({'message': message}), status


# @app.route('/add_pesticide_type', methods=['POST'])
# @token_required
# def add_pesticide_type(user_id):
#    if not is_authorized_user(user_id, [999]):
#        return jsonify({'error': 'Unauthorized access'}), 401
#
#   data = request.json
#   pesticide_name = data.get('pesticide_name')
#
#  if not pesticide_name:
#      return jsonify({'error': 'Pesticide name is required'}), 400
#
#  result = insert_pesticide_type(pesticide_name)
#  if isinstance(result, tuple):
#     return jsonify({'error': result[0]}), result[1]
#
#  return jsonify({'message': 'Pesticide type added successfully'})


# @app.route('/add_fertilizer_type', methods=['POST'])
# @token_required
# def add_fertilizer_type(user_id):
#    if not is_authorized_user(user_id, [999]):
#       return jsonify({'error': 'Unauthorized access'}), 401
#
#    data = request.json
#    fertilizer_name = data.get('fertilizer_name')
#
#    if not fertilizer_name:
#        return jsonify({'error': 'Fertilizer name is required'}), 400
#
#   result = insert_fertilizer_type(fertilizer_name)
#   if isinstance(result, tuple):
#      return jsonify({'error': result[0]}), result[1]
#
#  return jsonify({'message': 'Fertilizer type added successfully'})


@app.route('/add_massif', methods=['POST'])
@token_required
def add_massif(user_id):
    data = request.json
    crop_type_id = data.get('crop_type_id')
    sowing_time = data.get('sowing_time')  # 播种时间
    ex_harvest_time = data.get('ex_harvest_time')  # 预计收获时间
    massif_owner_id = data.get('massif_owner_id', user_id)  # 默认为当前用户ID

    # 验证用户权限
    if not is_authorized_user(user_id, [0, 999]):
        return jsonify({'error': 'Unauthorized access'}), 401

    # 如果用户不是管理员，只能为自己添加地块
    if not is_admin(user_id) and user_id != massif_owner_id:
        return jsonify({'error': 'Unauthorized to add massif for other users'}), 401

    # 调用db.py中的函数执行添加地块操作
    result = add_massif_record(crop_type_id, sowing_time, ex_harvest_time, massif_owner_id)
    if isinstance(result, tuple):
        return jsonify({'error': result[0]}), result[1]

    return jsonify({'message': 'Massif added successfully', 'massif_id': result})


@app.route('/update_massif', methods=['POST'])
@token_required
def update_massif(user_id):
    data = request.json
    massif_id = data.get('massif_id')
    new_crop_type_id = data.get('new_crop_type_id')
    new_sowing_time = data.get('new_sowing_time')
    new_ex_harvest_time = data.get('new_ex_harvest_time')

    # 验证用户权限
    if not is_authorized_user(user_id, [0, 999]) or not can_modify_massif(user_id, massif_id):
        return jsonify({'error': 'Unauthorized access'}), 401

    result = update_massif_record(massif_id, new_crop_type_id, new_sowing_time, new_ex_harvest_time)
    if isinstance(result, tuple):
        return jsonify({'error': result[0]}), result[1]

    return jsonify({'message': 'Massif updated successfully'})


@app.route('/delete_massif', methods=['POST'])
@token_required
def delete_massif(user_id):
    data = request.json
    massif_id = data.get('massif_id')

    # 验证用户权限
    if not is_authorized_user(user_id, [0, 999]) or not can_modify_massif(user_id, massif_id):
        return jsonify({'error': 'Unauthorized access'}), 401

    result = delete_massif_record(massif_id)
    if isinstance(result, tuple):
        return jsonify({'error': result[0]}), result[1]

    return jsonify({'message': 'Massif deleted successfully'})


@app.route('/delete_crop_type', methods=['POST'])
@token_required
def delete_crop_type(user_id):
    data = request.json
    crop_type_id = data.get('crop_type_id')

    # 验证用户是否为管理员
    if not is_admin(user_id):
        return jsonify({'error': 'Unauthorized access'}), 401

    # 调用 db.py 中的函数执行删除作物类型操作
    result = delete_crop_type_record(crop_type_id)
    if isinstance(result, tuple):
        return jsonify({'error': result[0]}), result[1]

    return jsonify({'message': 'Crop type deleted successfully'})


@app.route('/get_crop_types', methods=['GET'])
@token_required
def get_crop_types(current_user_id):
    crop_types = get_all_crop_types()
    if isinstance(crop_types, tuple):
        # 发生错误时
        return jsonify({'error': crop_types[0]}), 500
    return jsonify({'crop_types': crop_types})


@app.route('/add_harvest', methods=['POST'])
@token_required
def add_harvest(user_id):
    data = request.json
    massif_id = data.get('massif_id')
    harvest_quantity = data.get('harvest_quantity')
    harvest_time = data.get('harvest_time')  # 格式："YYYY-MM-DD HH:MM:SS"
    sowing_time = data.get('sowing_time')  # 格式："YYYY-MM-DD HH:MM:SS"

    # 验证用户权限
    if not is_authorized_user(user_id, [0, 999]):
        return jsonify({'error': 'Unauthorized access'}), 401

    # 如果用户不是管理员，验证地块是否属于该用户
    if not is_admin(user_id) and not is_user_massif(user_id, massif_id):
        return jsonify({'error': 'Unauthorized to add harvest for this massif'}), 401

    # 调用db.py中的函数执行添加收获操作
    result = add_harvest_record(massif_id, harvest_quantity, harvest_time, sowing_time)
    if isinstance(result, tuple):
        return jsonify({'error': result[0]}), result[1]

    return jsonify({'message': 'Harvest added successfully', 'harvest_id': result})


@app.route('/delete_harvest', methods=['POST'])
@token_required
def delete_harvest(user_id):
    data = request.json
    harvest_id = data.get('harvest_id')

    # 验证用户权限
    if not is_authorized_user(user_id, [0, 999]):
        return jsonify({'error': 'Unauthorized access'}), 401

    # 检查收获记录是否属于用户（仅对非管理员）
    if not is_admin(user_id) and not is_user_harvest(user_id, harvest_id):
        return jsonify({'error': 'Unauthorized to delete this harvest record'}), 401

    # 调用db.py中的函数执行删除操作
    result = delete_harvest_record(harvest_id)
    if isinstance(result, tuple):
        return jsonify({'error': result[0]}), result[1]

    return jsonify({'message': 'Harvest record deleted successfully'})


@app.route('/get_massifs', methods=['GET'])
@token_required
def get_massifs(user_id):
    # 检查用户类型
    if is_authorized_user(user_id, [999, 2]):
        # 管理员和政府用户可以获取所有地块信息
        massifs = get_all_massifs()
    elif is_authorized_user(user_id, [0]):
        # 农户只能获取属于自己的地块信息
        massifs = get_user_massifs(user_id)
    else:
        return jsonify({'error': 'Unauthorized access'}), 401

    return jsonify({'massifs': massifs})


@app.route('/add_irrigation', methods=['POST'])
@token_required
def add_irrigation(user_id):
    data = request.json
    massif_id = data.get('massif_id')
    irrigate_quantity = data.get('irrigate_quantity')
    irrigate_time = data.get('irrigate_time')  # 格式："YYYY-MM-DD HH:MM:SS"

    # 验证用户权限
    if not is_authorized_user(user_id, [0, 999]):
        return jsonify({'error': 'Unauthorized access'}), 401

    # 如果用户不是管理员，验证地块是否属于该用户
    if not is_admin(user_id) and not is_user_massif(user_id, massif_id):
        return jsonify({'error': 'Unauthorized to add irrigation for this massif'}), 401

    # 调用db.py中的函数执行添加灌溉操作
    result = add_irrigation_record(massif_id, irrigate_quantity, irrigate_time)
    if isinstance(result, tuple):
        return jsonify({'error': result[0]}), result[1]

    return jsonify({'message': 'Irrigation added successfully', 'irrigation_id': result})


@app.route('/delete_irrigation', methods=['POST'])
@token_required
def delete_irrigation(user_id):
    data = request.json
    irrigation_id = data.get('irrigation_id')

    # 检查用户是否有权限删除此灌溉记录
    if not can_delete_irrigation(user_id, irrigation_id):
        return jsonify({'error': 'Unauthorized to delete this irrigation record'}), 401

    # 调用db.py中的函数执行删除操作
    result = delete_irrigation_record(irrigation_id)
    if isinstance(result, tuple):
        return jsonify({'error': result[0]}), result[1]

    return jsonify({'message': 'Irrigation record deleted successfully'})


@app.route('/get_irrigations', methods=['GET'])
@token_required
def get_irrigations(user_id):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 检查用户类型
        cursor.execute("SELECT user_type FROM users WHERE user_id = %s;", (user_id,))
        user_type = cursor.fetchone()[0]

        # 查询视图获取全部的列
        if user_type in [999, 2]:  # 管理员或政府用户
            cursor.execute("SELECT * FROM view_irrigations ORDER BY massif_id;")
        else:  # 农户用户
            cursor.execute(
                "SELECT * FROM view_irrigations WHERE user_id = %s ORDER BY massif_id;",
                (user_id,))

        rows = cursor.fetchall()

        # 假设视图中的列顺序是：irrigation_id, crop_name, username, irrigate_quantity, irrigate_time, massif_id, user_id
        # 转换成字典列表
        irrigations = [
            {
                'irrigation_id': row[0],
                'crop_name': row[1],
                'username': row[2],
                'irrigate_quantity': row[3],
                'irrigate_time': row[4].strftime('%Y-%m-%d %H:%M') if isinstance(row[4], datetime.datetime) else None,
                'massif_id': row[5],
                'user_id': row[6]
            } for row in rows
        ]
        return jsonify({'irrigations': irrigations})

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/get_harvests', methods=['GET'])
@token_required
def get_harvests(user_id):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 检查用户类型
        cursor.execute("SELECT user_type FROM users WHERE user_id = %s;", (user_id,))
        user_type = cursor.fetchone()[0]

        if user_type in [999, 2]:  # 管理员或政府用户
            cursor.execute(
                "SELECT harvest_id, crop_name, username, harvest_quantity, sowing_time, harvest_time FROM view_harvests ORDER BY user_id;")
        else:  # 农户用户
            cursor.execute(
                "SELECT harvest_id, crop_name, username, harvest_quantity, sowing_time, harvest_time FROM view_harvests WHERE user_id = %s ORDER BY user_id;",
                (user_id,))

        rows = cursor.fetchall()
        # 转换成字典列表
        harvests = [
            {
                'harvest_id': row[0],
                'crop_name': row[1],
                'username': row[2],
                'harvest_quantity': row[3],
                'sowing_time': row[4].strftime('%Y-%m-%d') if row[4] else None,
                'harvest_time': row[5].strftime('%Y-%m-%d') if row[5] else None
            } for row in rows
        ]
        return jsonify({'harvests': harvests})

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/add_sale', methods=['POST'])
@token_required
def add_sale(user_id):
    data = request.json
    crop_type_id = data.get('crop_type_id')
    quantity = data.get('quantity')
    sale = data.get('sale')  # 假设这是销售价格

    # 获取用户类型
    user_type = get_user_type(user_id)

    # 检查用户是否有权限
    if user_type in [0, 999]:
        result = add_sale_record(user_id, crop_type_id, quantity, sale)
        if isinstance(result, tuple):
            return jsonify({'error': result[0]}), result[1]

        return jsonify({'message': 'Sale added successfully', 'sale_id': result})

    return jsonify({'error': 'Unauthorized user type'}), 401


@app.route('/delete_sale', methods=['POST'])
@token_required
def delete_sale(user_id):
    data = request.json
    sale_id = data.get('sale_id')

    if not sale_id:
        return jsonify({'error': 'Sale ID is required'}), 400

    result = delete_sale_record(user_id, sale_id)
    if isinstance(result, tuple):
        return jsonify({'error': result[0]}), result[1]

    return jsonify({'message': result})


@app.route('/add_purchase', methods=['POST'])
@token_required
def add_purchase(user_id):
    data = request.json
    sale_id = data.get('sale_id')  # 销售记录的ID
    sale_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 验证用户权限，只有粮企（user_type 1）可以添加购买记录
    if not is_authorized_user(user_id, [1]):
        return jsonify({'error': 'Unauthorized access'}), 401

    # 调用函数添加购买记录
    result = add_purchase_record(sale_id, user_id, sale_time)
    if isinstance(result, tuple):
        return jsonify({'error': result[0]}), result[1]

    return jsonify({'message': 'Purchase record added successfully'})


@app.route('/view_sales_market', methods=['GET'])
@token_required
def view_sales_market(user_id):
    # 验证用户权限，只有政府（user_type 1）可以访问
    if not is_authorized_user(user_id, [1]):
        return jsonify({'error': 'Unauthorized access'}), 401

    return jsonify(get_sales_market_info())


@app.route('/get_all_sales', methods=['GET'])
@token_required
def get_all_sales(current_user_id):
    # 获取当前用户的ID，这里假设从请求头部中获取用户ID

    # 验证用户类型
    if not is_authorized_user(current_user_id, [999, 2]):
        return jsonify({'error': 'Unauthorized access'}), 401

    # 从数据库获取销售记录
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection error'}), 500

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM view_all_sales;")
    sales_records = cursor.fetchall()
    conn.close()

    # 格式化返回的数据
    sales_data = []
    for record in sales_records:
        sales_data.append({
            "sale_id": record[0],
            "seller_username": record[1],
            "buyer_username": record[2],
            "crop_name": record[3],
            "quantity": record[4],
            "price": record[5],
            "begin_time": record[6],
            "sale_time": record[7]
        })

    return jsonify(sales_data)


@app.route('/get_personal_sales', methods=['GET'])
@token_required
def get_farmer_sales(user_id):
    if not user_id:
        return jsonify({'error': 'User ID is required'}), 400

    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        query = "SELECT * FROM view_farmer_sales WHERE saler_id = %s;"
        cursor.execute(query, (user_id,))
        sales_records = cursor.fetchall()

        # 格式化返回的数据
        sales_data = []
        for record in sales_records:
            sales_data.append({
                "sale_id": record["sale_id"],
                "crop_type_id": record["crop_type_id"],
                "crop_name": record["crop_name"],
                "quantity": record["quantity"],
                "sale_time": record["sale_time"],
                "begin_time": record["begin_time"],
                "buyer_id": record["buyer_id"],
                "saler_id": record["saler_id"],
                "sale_type": record["sale_type"],
                "sale": record["sale"]
            })
        return jsonify(sales_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/get_all_users', methods=['GET'])
@token_required
def get_all_users(current_user_id):
    # 验证用户是否为管理员
    if not is_admin(current_user_id):
        return jsonify({'error': 'Unauthorized access'}), 401

    # 获取所有用户信息
    try:
        users_info = get_all_users_info()
        return jsonify(users_info)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5001)
