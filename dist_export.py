import os
import cx_Oracle
import traceback
import xlsxwriter
from flask import (
    Flask, request, render_template, Blueprint,
    send_from_directory
)
from datetime import datetime


app = Flask(__name__)
bpin = Blueprint('in', __name__, url_prefix='/maintain/distIn', static_folder='static')
bpout = Blueprint('out', __name__, url_prefix='/maintain/distOut', static_folder='static')

username = os.getenv('ORCL_USERNAME') or 'username'
password = os.getenv('ORCL_PASSWORD') or 'password'
dbUrl = os.getenv('ORCL_DBURL') or '127.0.0.1:1521/orcl'


def executeSql(sql, **kw):
    con = cx_Oracle.connect(username, password, dbUrl)
    cursor = con.cursor()
    result = None
    try:
        cursor.prepare(sql)
        cursor.execute(None, kw)
        result = cursor.fetchall()
        con.commit()
    except Exception:
        traceback.print_exc()
        con.rollback()
    finally:
        cursor.close()
        con.close()
    return result


@bpin.route('/export/<distno>', methods=['GET'])
def inExport(distno):
    now = datetime.now()
    xlsxDir = "export"
    fileName = "{}_{}_in.xlsx".format(distno, now.strftime('%Y%m%d%H%M%S'))
    print("fileName is: {}".format(fileName))
    wb = xlsxwriter.Workbook(os.path.join(xlsxDir, fileName))
    sql = '''
    select t.ebc_code, t.order_no, t.logistics_code, t.logistics_no, t.agent_code, t.invt_no, t.customs_code
    from ceb2_invt_head t
    left outer join pre_dist_bill_list t1 on t1.bill_no = t.invt_no
    where t.app_status = '800'
    and (t.cus_status not in ('26', '24') or t.cus_status is null)
    and t1.dist_no = :distno
    '''
    result = executeSql(sql, distno=distno)
    sht1 = wb.add_worksheet('内网未放行')
    sht1.write_string(0, 0, '电商企业')
    sht1.write_string(0, 1, '订单号')
    sht1.write_string(0, 2, '物流企业')
    sht1.write_string(0, 3, '运单号')
    sht1.write_string(0, 4, '报关企业')
    sht1.write_string(0, 5, '清单号')
    sht1.write_string(0, 6, '关区号')

    row = 1
    for invt in result:
        sht1.write_string(row, 0, invt[0])
        sht1.write_string(row, 1, invt[1])
        sht1.write_string(row, 2, invt[2])
        sht1.write_string(row, 3, invt[3])
        sht1.write_string(row, 4, invt[4])
        sht1.write_string(row, 5, invt[5])
        sht1.write_string(row, 6, invt[6])
        row += 1

    sql = '''
    select t.ebc_code, t.order_no, t.logistics_code, t.logistics_no, t.agent_code, t.invt_no, t.customs_code
    from ceb2_invt_head t
    left outer join pre_dist_bill_list t1 on t1.bill_no = t.invt_no
    where t.app_status = '800'
    and t.cus_status in ('26', '24')
    and t1.dist_no = :distno
    '''
    result = executeSql(sql, distno=distno)

    sht2 = wb.add_worksheet('内网已放行')
    sht2.write_string(0, 0, '电商企业')
    sht2.write_string(0, 1, '订单号')
    sht2.write_string(0, 2, '物流企业')
    sht2.write_string(0, 3, '运单号')
    sht2.write_string(0, 4, '报关企业')
    sht2.write_string(0, 5, '清单号')
    sht2.write_string(0, 6, '关区号')

    row = 1
    for invt in result:
        sht2.write_string(row, 0, invt[0])
        sht2.write_string(row, 1, invt[1])
        sht2.write_string(row, 2, invt[2])
        sht2.write_string(row, 3, invt[3])
        sht2.write_string(row, 4, invt[4])
        sht2.write_string(row, 5, invt[5])
        sht2.write_string(row, 6, invt[6])
        row += 1

    wb.close()
    return send_from_directory(xlsxDir, fileName, as_attachment=True)


@bpout.route('/export/<distno>', methods=['GET'])
def inExport(distno):
    now = datetime.now()
    xlsxDir = "export"
    fileName = "{}_{}_out.xlsx".format(distno, now.strftime('%Y%m%d%H%M%S'))
    print("fileName is: {}".format(fileName))
    wb = xlsxwriter.Workbook(os.path.join(xlsxDir, fileName))
    sql = '''
    select t.ebc_code, t.order_no, t.logistics_code, t.logistics_no, t.agent_code, t.invt_no, t.customs_code
    from ceb3_invt_head t
    left outer join pre_dist_bill_list t1 on t1.bill_no = t.invt_no
    where t.app_status = '800'
    and (t.cus_status not in ('26', '24') or t.cus_status is null)
    and t1.dist_no = :distno
    '''
    result = executeSql(sql, distno=distno)
    sht1 = wb.add_worksheet('内网未放行')
    sht1.write_string(0, 0, '电商企业')
    sht1.write_string(0, 1, '订单号')
    sht1.write_string(0, 2, '物流企业')
    sht1.write_string(0, 3, '运单号')
    sht1.write_string(0, 4, '报关企业')
    sht1.write_string(0, 5, '清单号')
    sht1.write_string(0, 6, '关区号')

    row = 1
    for invt in result:
        sht1.write_string(row, 0, invt[0])
        sht1.write_string(row, 1, invt[1])
        sht1.write_string(row, 2, invt[2])
        sht1.write_string(row, 3, invt[3])
        sht1.write_string(row, 4, invt[4])
        sht1.write_string(row, 5, invt[5])
        sht1.write_string(row, 6, invt[6])
        row += 1

    sql = '''
    select t.ebc_code, t.order_no, t.logistics_code, t.logistics_no, t.agent_code, t.invt_no, t.customs_code
    from ceb3_invt_head t
    left outer join pre_dist_bill_list t1 on t1.bill_no = t.invt_no
    where t.app_status = '800'
    and t.cus_status in ('26', '24')
    and t1.dist_no = :distno
    '''
    result = executeSql(sql, distno=distno)

    sht2 = wb.add_worksheet('内网已放行')
    sht2.write_string(0, 0, '电商企业')
    sht2.write_string(0, 1, '订单号')
    sht2.write_string(0, 2, '物流企业')
    sht2.write_string(0, 3, '运单号')
    sht2.write_string(0, 4, '报关企业')
    sht2.write_string(0, 5, '清单号')
    sht2.write_string(0, 6, '关区号')

    row = 1
    for invt in result:
        sht2.write_string(row, 0, invt[0])
        sht2.write_string(row, 1, invt[1])
        sht2.write_string(row, 2, invt[2])
        sht2.write_string(row, 3, invt[3])
        sht2.write_string(row, 4, invt[4])
        sht2.write_string(row, 5, invt[5])
        sht2.write_string(row, 6, invt[6])
        row += 1

    wb.close()
    return send_from_directory(xlsxDir, fileName, as_attachment=True)


app.register_blueprint(bpin)
app.register_blueprint(bpout)