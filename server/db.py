import psycopg2
import datetime

# 数据库配置信息
DB_CONFIG = {
    'host': '1.92.68.193',
    'port': '26000',
    'database': 'farm',
    'user': 'ad',
    'password': 'ad@123123'
}


def get_db_connection():
    """创建数据库连接"""
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        dbname=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )
    return conn


def init_db():
    """初始化数据库，创建表等"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # 列出所有创建表的 SQL 语句
    sql_statements = [
        '''
CREATE TABLE IF NOT EXISTS user_groups (
    group_id SERIAL PRIMARY KEY,
    group_name VARCHAR(255) NOT NULL,
    group_description TEXT
);
        ''',
        '''
CREATE TABLE IF NOT EXISTS crop_types (
    crop_type_id SERIAL PRIMARY KEY,
    crop_name VARCHAR(255) NOT NULL
);
        ''',
        '''
CREATE TABLE IF NOT EXISTS organizations (
    organization_id SERIAL PRIMARY KEY,
    organization_name VARCHAR(255),
    organization_type VARCHAR(255),
    additional_info TEXT
);
        ''',
        '''
CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(255),
    phonenumber VARCHAR(255),
    organization_id INT REFERENCES organizations(organization_id),
    useremail VARCHAR(255),
    password VARCHAR(255),
    user_online INT,
    user_lastonline TIMESTAMP,
    user_type INT REFERENCES user_groups(group_id)
);
        ''',
        '''
CREATE TABLE IF NOT EXISTS massifs (
    massif_id SERIAL PRIMARY KEY,
    crop_type_id INT REFERENCES crop_types(crop_type_id),
    sowing_time TIMESTAMP,
    ex_harvest_time TIMESTAMP,
    user_id INT REFERENCES users(user_id)
);
        ''',
        '''
CREATE TABLE IF NOT EXISTS harvests (
    harvest_id SERIAL PRIMARY KEY,
    massif_id INT REFERENCES massifs(massif_id),
    harvest_quantity FLOAT,
    sowing_time TIMESTAMP,
    harvest_time TIMESTAMP
);
        ''',
        '''
CREATE TABLE IF NOT EXISTS irrigations (
    irrigation_id SERIAL PRIMARY KEY,
    massif_id INT REFERENCES massifs(massif_id),
    irrigate_quantity FLOAT,
    irrigate_time TIMESTAMP
);
        ''',
        '''
CREATE TABLE IF NOT EXISTS sales (
    sale_id SERIAL PRIMARY KEY,
    crop_type_id INT REFERENCES crop_types(crop_type_id),
    quantity FLOAT,
    sale_time TIMESTAMP,
    begin_time TIMESTAMP,
    buyer_id INT REFERENCES users(user_id),
    saler_id INT REFERENCES users(user_id),
    sale_type INT,
    sale FLOAT
);
        ''',
    ]
    # 检查并插入用户组
    cursor.execute("SELECT group_id FROM user_groups WHERE group_id = 999;")
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO user_groups (group_id, group_name, group_description) VALUES (999, 'Admin', 'Administrator group with full privileges');"
        )

    # 检查并插入组织
    cursor.execute("SELECT organization_id FROM organizations WHERE organization_id = 1;")
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO organizations (organization_id, organization_name, organization_type, additional_info) VALUES (1, 'Admin Organization', 'Government', 'Organization for administrative users');"
        )
    # 插入用户组
    cursor.execute("SELECT group_id FROM user_groups WHERE group_id = 0;")
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO user_groups (group_id, group_name, group_description) VALUES (0, 'Farmer', 'Group for farmers');"
        )

    cursor.execute("SELECT group_id FROM user_groups WHERE group_id = 1;")
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO user_groups (group_id, group_name, group_description) VALUES (1, 'Grain Company', 'Group for grain companies');"
        )

    cursor.execute("SELECT group_id FROM user_groups WHERE group_id = 2;")
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO user_groups (group_id, group_name, group_description) VALUES (2, 'Government', 'Group for government entities');"
        )

    # 插入组织
    cursor.execute("SELECT organization_id FROM organizations WHERE organization_id = 1;")
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO organizations (organization_id, organization_name, organization_type, additional_info) VALUES (1, 'Admin Organization', 'Government', 'Organization for administrative users');"
        )

    # 检查并插入管理员用户
    cursor.execute("SELECT user_id FROM users WHERE user_id = 1;")
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO users (user_id, username, phonenumber, organization_id, useremail, password, user_online, user_lastonline, user_type) VALUES (1, 'admin', '1234567890', 1, 'admin@example.com', 'adminadmin', 0, CURRENT_TIMESTAMP, 999);"
        )

    conn.commit()  # 确保提交事务
    cursor.close()
    conn.close()


def list_tables():
    """列出数据库中的所有表"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables = cursor.fetchall()
        return [table[0] for table in tables]
    except Exception as e:
        print(f"Error querying database: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def register_user(username, phonenumber, organization, useremail, password, user_type, user_online=1):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 检查手机号或邮箱是否已存在
        check_sql = "SELECT user_id FROM users WHERE phonenumber = %s OR useremail = %s;"
        cursor.execute(check_sql, (phonenumber, useremail))
        if cursor.fetchone():
            return "User already exists", 400  # 用户已存在

        # 用户不存在，插入新用户数据
        user_lastonline = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_sql = '''
            INSERT INTO users (username, phonenumber, organization_id, useremail, password, user_online, user_lastonline, user_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        '''
        cursor.execute(insert_sql, (
            username, phonenumber, organization, useremail, password, user_online, user_lastonline, user_type))
        conn.commit()  # 确保提交事务
        return user_lastonline
    except Exception as e:
        conn.rollback()  # 发生错误时回滚事务
        print(f"Database error: {e}")
        return str(e)
    finally:
        cursor.close()
        conn.close()


def authenticate_user(login, password):
    """验证用户登录信息"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 查找用户（通过用户名、手机号或邮箱）
        user_query = '''
            SELECT u.*, o.organization_name, o.organization_type 
            FROM users u
            JOIN organizations o ON u.organization_id = o.organization_id
            WHERE (u.username = %s OR u.phonenumber = %s OR u.useremail = %s) AND u.password = %s;
        '''
        cursor.execute(user_query, (login, login, login, password))
        user = cursor.fetchone()
        if user:
            # 更新用户在线状态和最后登录时间
            update_query = '''
                UPDATE users 
                SET user_online = 1, user_lastonline = NOW() 
                WHERE user_id = %s;
            '''
            cursor.execute(update_query, (user[0],))
            conn.commit()
            return user
        else:
            return None
    except Exception as e:
        return str(e)
    finally:
        cursor.close()
        conn.close()


def logout_user(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 检查用户当前的在线状态
        status_check_query = 'SELECT user_online FROM users WHERE user_id = %s;'
        cursor.execute(status_check_query, (user_id,))
        user_status = cursor.fetchone()

        if user_status and user_status[0] == 0:
            return 'Already logged out'

        # 用户在线，更新状态为离线
        update_query = 'UPDATE users SET user_online = 0 WHERE user_id = %s;'
        cursor.execute(update_query, (user_id,))
        conn.commit()
        return user_id
    except Exception as e:
        return str(e)
    finally:
        cursor.close()
        conn.close()


def check_user_online_status(user_id):
    """ 检查用户的在线状态 """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT user_online FROM users WHERE user_id = %s;", (user_id,))
        user_status = cursor.fetchone()
        return user_status and user_status[0] == 1
    except:
        return False
    finally:
        cursor.close()
        conn.close()


def update_user_info(user_id, new_username=None, new_phonenumber=None, new_useremail=None, new_password=None):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 检查手机号和电子邮箱是否已存在
        if new_phonenumber or new_useremail:
            query = "SELECT user_id FROM users WHERE (phonenumber = %s OR useremail = %s) AND user_id != %s;"
            cursor.execute(query, (new_phonenumber, new_useremail, user_id))
            if cursor.fetchone():
                return "Phone number or email already exists", 400

        # 构建更新语句
        updates = []
        params = []
        if new_username:
            updates.append("username = %s")
            params.append(new_username)
        if new_phonenumber:
            updates.append("phonenumber = %s")
            params.append(new_phonenumber)
        if new_useremail:
            updates.append("useremail = %s")
            params.append(new_useremail)
        if new_password:
            updates.append("password = %s")  # 在实际应用中，应对密码进行加密处理
            params.append(new_password)

        update_query = "UPDATE users SET " + ", ".join(updates) + " WHERE user_id = %s;"
        params.append(user_id)
        cursor.execute(update_query, tuple(params))
        conn.commit()
        return "User info updated successfully"

    except Exception as e:
        if conn:
            conn.rollback()
        return str(e)
    finally:
        if conn:
            conn.close()


def manage_organization(action, organization_id=None, name=None, type=None, info=None):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if action == 'add':
            # 检查名称是否已存在
            cursor.execute("SELECT organization_id FROM organizations WHERE organization_name = %s;", (name,))
            if cursor.fetchone():
                return "Organization name already exists", 400

            # 添加新组织
            cursor.execute(
                "INSERT INTO organizations (organization_name, organization_type, additional_info) VALUES (%s, %s, %s);",
                (name, type, info))

        elif action == 'update':
            # 检查组织是否存在
            cursor.execute("SELECT organization_id FROM organizations WHERE organization_id = %s;", (organization_id,))
            if not cursor.fetchone():
                return "Organization not found", 404

            # 检查新名称是否已存在
            cursor.execute(
                "SELECT organization_id FROM organizations WHERE organization_name = %s AND organization_id != %s;",
                (name, organization_id))
            if cursor.fetchone():
                return "Another organization with the same name already exists", 400

            # 更新组织信息
            cursor.execute(
                "UPDATE organizations SET organization_name = %s, organization_type = %s, additional_info = %s WHERE organization_id = %s;",
                (name, type, info, organization_id))

        elif action == 'delete':
            # 检查组织是否存在
            cursor.execute("SELECT organization_id FROM organizations WHERE organization_id = %s;", (organization_id,))
            if not cursor.fetchone():
                return "Organization not found", 404

            # 删除组织
            cursor.execute("DELETE FROM organizations WHERE organization_id = %s;", (organization_id,))

        conn.commit()
        return "Operation successful"

    except Exception as e:
        if conn:
            conn.rollback()
        return f"Error during operation: {e}"
    finally:
        if conn:
            conn.close()


def is_admin(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT user_type FROM users WHERE user_id = %s;", (user_id,))
        user_type = cursor.fetchone()
        return user_type and user_type[0] == 999
    finally:
        cursor.close()
        conn.close()


def delete_organization_record(organization_name):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 检查组织是否存在
        cursor.execute("SELECT organization_id FROM organizations WHERE organization_name = %s;", (organization_name,))
        if not cursor.fetchone():
            return "Organization not found", 404

        # 删除组织
        cursor.execute("DELETE FROM organizations WHERE organization_name = %s;", (organization_name,))
        conn.commit()
        return "Organization deleted successfully"

    except Exception as e:
        if conn:
            conn.rollback()
        return str(e)
    finally:
        if conn:
            conn.close()


def fetch_organizations():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM organizations;")
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            organizations = [dict(zip(columns, row)) for row in rows]
            return organizations
    except Exception as e:
        return str(e), 500
    finally:
        conn.close()


def is_authorized_user(user_id, allowed_types):
    """检查用户是否为允许的用户类型之一"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT user_type FROM users WHERE user_id = %s;", (user_id,))
            user_type = cursor.fetchone()
            return user_type and user_type[0] in allowed_types
    finally:
        conn.close()


def get_farmer_massifs_view_data(user_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM view_farmer_massifs WHERE user_id = %s;", (user_id,))
            result = cursor.fetchall()
            return format_result(result)
    except Exception as e:
        raise e
    finally:
        conn.close()


def format_result(result_set):
    formatted_result = []
    for row in result_set:
        formatted_result.append({
            'massif_id': row[0],
            'crop_type': row[1],
            'harvest_count': row[2],
            'irrigation_count': row[3],
            'fertilization_count': row[4],
            'pesticide_count': row[5]
        })
    return formatted_result


def add_crop_type_db(crop_name):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 检查作物名称是否已存在
            cursor.execute("SELECT * FROM crop_types WHERE crop_name = %s;", (crop_name,))
            if cursor.fetchone():
                return "Crop type already exists", 400

            # 插入新的作物类型
            cursor.execute("INSERT INTO crop_types (crop_name) VALUES (%s);", (crop_name,))
            conn.commit()
            return "Crop type added successfully", 200
    except Exception as e:
        conn.rollback()
        return str(e), 500
    finally:
        conn.close()


# def insert_pesticide_type(pesticide_name):
#    conn = get_db_connection()
#   try:
#       with conn.cursor() as cursor:
#          cursor.execute("SELECT pesticide_type_id FROM pesticide_types WHERE pesticide_name = %s;",
#                         (pesticide_name,))
#          if cursor.fetchone():
#              return "Pesticide type name already exists", 400
#
#           cursor.execute("INSERT INTO pesticide_types (pesticide_name) VALUES (%s);", (pesticide_name,))
#           conn.commit()
#       return "Operation successful"
#   except Exception as e:
#      conn.rollback()
#      return str(e)
#   finally:
#      conn.close()


# def insert_fertilizer_type(fertilizer_name):
#    conn = get_db_connection()
#   try:
#     with conn.cursor() as cursor:
#           cursor.execute("SELECT fertilizer_type_id FROM fertilizer_types WHERE fertilizer_name = %s;",
#                         (fertilizer_name,))
#         if cursor.fetchone():
#            return "Fertilizer type name already exists", 400
#
#         cursor.execute("INSERT INTO fertilizer_types (fertilizer_name) VALUES (%s);", (fertilizer_name,))
#          conn.commit()
#       return "Operation successful"
#    except Exception as e:
#       conn.rollback()
#       return str(e)
#   finally:
#       conn.close()


def add_massif_record(crop_type_id, sowing_time, ex_harvest_time, massif_owner_id):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 插入地块记录
        insert_sql = '''
            INSERT INTO massifs (crop_type_id, sowing_time, ex_harvest_time, user_id)
            VALUES (%s, %s, %s, %s) RETURNING massif_id;
        '''
        cursor.execute(insert_sql, (crop_type_id, sowing_time, ex_harvest_time, massif_owner_id))
        massif_id = cursor.fetchone()[0]  # 获取新插入记录的ID
        conn.commit()
        return massif_id
    except Exception as e:
        if conn:
            conn.rollback()
        return str(e)
    finally:
        if conn:
            conn.close()


def update_massif_record(massif_id, new_crop_type_id, new_sowing_time, new_ex_harvest_time):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 更新地块记录
        update_sql = '''
            UPDATE massifs
            SET crop_type_id = %s, sowing_time = %s, ex_harvest_time = %s
            WHERE massif_id = %s;
        '''
        cursor.execute(update_sql, (new_crop_type_id, new_sowing_time, new_ex_harvest_time, massif_id))
        conn.commit()
        return "Massif updated"
    except Exception as e:
        if conn:
            conn.rollback()
        return str(e), 500
    finally:
        if conn:
            conn.close()


def delete_massif_record(massif_id):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 删除地块记录
        delete_sql = "DELETE FROM massifs WHERE massif_id = %s;"
        cursor.execute(delete_sql, (massif_id,))
        conn.commit()
        return "Massif deleted"
    except Exception as e:
        if conn:
            conn.rollback()
        return str(e), 500
    finally:
        if conn:
            conn.close()


def can_modify_massif(user_id, massif_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 检查当前用户是否为地块所有者或管理员
            cursor.execute("SELECT user_id FROM massifs WHERE massif_id = %s;", (massif_id,))
            massif_owner_id = cursor.fetchone()
            if massif_owner_id and (user_id == massif_owner_id[0] or is_admin(user_id)):
                return True
            else:
                return False
    finally:
        conn.close()


def delete_crop_type_record(crop_type_id):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 删除作物类型记录
        cursor.execute("DELETE FROM crop_types WHERE crop_type_id = %s;", (crop_type_id,))
        conn.commit()

        return "Crop type deleted successfully"
    except Exception as e:
        if conn:
            conn.rollback()
        return str(e), 500
    finally:
        if conn:
            conn.close()


def get_all_crop_types():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM crop_types;")
        crop_types = cursor.fetchall()
        # 转换为字典列表
        crop_types_list = [{'crop_type_id': row[0], 'crop_name': row[1]} for row in crop_types]
        return crop_types_list
    except Exception as e:
        return str(e), 500
    finally:
        if conn:
            conn.close()


def add_harvest_record(massif_id, harvest_quantity, harvest_time, sowing_time):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 插入收获记录
        insert_sql = '''
            INSERT INTO harvests (massif_id, harvest_quantity, harvest_time, sowing_time)
            VALUES (%s, %s, %s, %s) RETURNING harvest_id;
        '''
        cursor.execute(insert_sql, (massif_id, harvest_quantity, harvest_time, sowing_time))
        harvest_id = cursor.fetchone()[0]  # 获取新插入记录的ID
        conn.commit()
        return harvest_id
    except Exception as e:
        if conn:
            conn.rollback()
        return str(e)
    finally:
        if conn:
            conn.close()


def is_user_massif(user_id, massif_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT user_id FROM massifs WHERE massif_id = %s;", (massif_id,))
            owner_id = cursor.fetchone()
            return owner_id and owner_id[0] == user_id
    finally:
        conn.close()


def is_user_harvest(user_id, harvest_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT u.user_id FROM harvests h JOIN massifs m ON h.massif_id = m.massif_id JOIN users u ON m.user_id = u.user_id WHERE h.harvest_id = %s;",
                (harvest_id,))
            result = cursor.fetchone()
            return result and result[0] == user_id
    finally:
        conn.close()


def delete_harvest_record(harvest_id):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 删除收获记录
        delete_sql = "DELETE FROM harvests WHERE harvest_id = %s;"
        cursor.execute(delete_sql, (harvest_id,))
        conn.commit()
        return "Harvest record deleted successfully"
    except Exception as e:
        if conn:
            conn.rollback()
        return str(e)
    finally:
        if conn:
            conn.close()


def get_user_massifs(user_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 第一步：获取特定用户的所有地块信息
            cursor.execute("SELECT * FROM view_massifs_info WHERE user_id = %s;", (user_id,))
            rows = cursor.fetchall()
            massifs = []
            for row in rows:
                # 第二步：对于每个地块，根据 crop_type 查询 crop_type_id
                crop_type_query = "SELECT crop_type_id FROM crop_types WHERE crop_name = %s;"
                cursor.execute(crop_type_query, (row[1],))
                crop_type_id_row = cursor.fetchone()
                crop_type_id = crop_type_id_row[0] if crop_type_id_row else None

                # 将信息加入列表
                massifs.append({
                    'massif_id': row[0],
                    'crop_type': row[1],
                    'planting_date': row[2].strftime('%Y-%m-%d'),
                    'harvest_date': row[3].strftime('%Y-%m-%d'),
                    'owner': row[4],
                    'user_id': row[5],
                    'crop_type_id': crop_type_id  # 添加了 crop_type_id
                })
            return massifs
    finally:
        conn.close()


def get_all_massifs():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 第一步：获取所有地块信息
            cursor.execute("SELECT * FROM view_massifs_info;")
            rows = cursor.fetchall()
            massifs = []
            for row in rows:
                # 第二步：对于每个地块，根据 crop_type 查询 crop_type_id
                crop_type_query = "SELECT crop_type_id FROM crop_types WHERE crop_name = %s;"
                cursor.execute(crop_type_query, (row[1],))
                crop_type_id_row = cursor.fetchone()
                crop_type_id = crop_type_id_row[0] if crop_type_id_row else None

                # 将信息加入列表
                massifs.append({
                    'massif_id': row[0],
                    'crop_type': row[1],
                    'planting_date': row[2].strftime('%Y-%m-%d'),
                    'harvest_date': row[3].strftime('%Y-%m-%d'),
                    'owner': row[4],
                    'user_id': row[5],
                    'crop_type_id': crop_type_id  # 添加了 crop_type_id
                })
            return massifs
    finally:
        conn.close()


def add_irrigation_record(massif_id, irrigate_quantity, irrigate_time):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 插入灌溉记录
        insert_sql = '''
            INSERT INTO irrigations (massif_id, irrigate_quantity, irrigate_time)
            VALUES (%s, %s, %s) RETURNING irrigation_id;
        '''
        cursor.execute(insert_sql, (massif_id, irrigate_quantity, irrigate_time))
        irrigation_id = cursor.fetchone()[0]  # 获取新插入记录的ID
        conn.commit()
        return irrigation_id
    except Exception as e:
        if conn:
            conn.rollback()
        return str(e)
    finally:
        if conn:
            conn.close()


def delete_irrigation_record(irrigation_id):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 删除指定的灌溉记录
        delete_sql = "DELETE FROM irrigations WHERE irrigation_id = %s;"
        cursor.execute(delete_sql, (irrigation_id,))
        conn.commit()
        return "Irrigation record deleted successfully"
    except Exception as e:
        if conn:
            conn.rollback()
        return str(e)
    finally:
        if conn:
            conn.close()


def can_delete_irrigation(user_id, irrigation_id):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 首先检查用户是否是管理员（user_type = 999）
        cursor.execute("SELECT user_type FROM users WHERE user_id = %s;", (user_id,))
        user_type = cursor.fetchone()[0]
        if user_type == 999:
            return True  # 管理员可以删除任何记录

        # 如果不是管理员，检查灌溉记录是否属于该用户的地块
        cursor.execute("""
            SELECT i.irrigation_id FROM irrigations i
            JOIN massifs m ON i.massif_id = m.massif_id
            WHERE i.irrigation_id = %s AND m.user_id = %s;
        """, (irrigation_id, user_id))
        return cursor.fetchone() is not None  # 如果找到记录，说明用户有权删除

    except Exception as e:
        print(str(e))
        return False
    finally:
        if conn:
            conn.close()


def add_sale_record(saler_id, crop_type_id, quantity, sale):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 插入销售记录，卖家挂单
        insert_sql = '''
            INSERT INTO sales (crop_type_id, quantity, begin_time, saler_id, sale_type, sale)
            VALUES (%s, %s, CURRENT_TIMESTAMP, %s, 0, %s) RETURNING sale_id;
        '''
        cursor.execute(insert_sql, (crop_type_id, quantity, saler_id, sale))
        sale_id = cursor.fetchone()[0]  # 获取新插入记录的ID
        conn.commit()
        return sale_id

    except Exception as e:
        if conn:
            conn.rollback()
        return str(e)
    finally:
        if conn:
            conn.close()


def get_user_type(user_id):
    """获取用户类型"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT user_type FROM users WHERE user_id = %s;", (user_id,))
            user_type = cursor.fetchone()
            if user_type:
                return user_type[0]
    finally:
        conn.close()
    return None


def delete_sale_record(user_id, sale_id):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 首先检查销售记录是否存在
        cursor.execute("SELECT saler_id FROM sales WHERE sale_id = %s;", (sale_id,))
        record = cursor.fetchone()
        if not record:
            return "Sale record not found", 404

        # 如果用户不是管理员且不是销售记录的拥有者，则拒绝操作
        if not is_admin(user_id) and user_id != record[0]:
            return "Unauthorized to delete this sale record", 401

        # 删除销售记录
        cursor.execute("DELETE FROM sales WHERE sale_id = %s;", (sale_id,))
        conn.commit()
        return "Sale record deleted successfully"

    except Exception as e:
        if conn:
            conn.rollback()
        return str(e)
    finally:
        if conn:
            conn.close()


def add_purchase_record(sale_id, buyer_id, sale_time):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 更新销售记录为购买状态
        update_sql = '''
            UPDATE sales 
            SET buyer_id = %s, sale_time = %s, sale_type = 1 
            WHERE sale_id = %s;
        '''
        cursor.execute(update_sql, (buyer_id, sale_time, sale_id))
        conn.commit()
        return "Purchase record added successfully"
    except Exception as e:
        if conn:
            conn.rollback()
        return str(e), 400
    finally:
        if conn:
            conn.close()


def get_sales_market_info():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 查询待销售市场信息
        query_sql = '''
            SELECT * FROM view_sales_market
        '''
        cursor.execute(query_sql)
        market_info = cursor.fetchall()

        # 将每行转换为字典
        market_info_dicts = [
            {
                "sale_id": row[0],
                "seller_username": row[1],
                "crop_name": row[2],
                "quantity": row[3],
                "sale": row[4],
                "begin_time": row[5]
            }
            for row in market_info
        ]
        return market_info_dicts
    except Exception as e:
        return str(e), 400
    finally:
        if conn:
            conn.close()


def get_all_users_info():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM users;")
        users = cursor.fetchall()
        # 获取列名称
        col_names = [desc[0] for desc in cursor.description]
        # 格式化结果为字典列表
        user_list = [dict(zip(col_names, user)) for user in users]
        return user_list
    finally:
        cursor.close()
        conn.close()


def get_user_id_by_username(username):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM users WHERE username = %s;", (username,))
        user_id = cursor.fetchone()
        return user_id[0] if user_id else None
    except Exception as e:
        return None
    finally:
        if conn:
            conn.close()


init_db()
