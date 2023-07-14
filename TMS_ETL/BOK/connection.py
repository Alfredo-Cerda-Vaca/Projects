"""Connection from SQL."""

# External dependencies
import sys
import pyodbc # DB Connection
from dotenv import dotenv_values # Read .env files

sys.path.insert(0, '../')
# pylint: disable=C0413
#from logger import log

DB_DRIVER = dotenv_values().get('DB_DRIVER')
DB_SERVER = dotenv_values().get('DB_SERVER')
DB_NAME = dotenv_values().get('DB_NAME')
DB_USERNAME = dotenv_values().get('DB_USERNAME')
DB_PASSWORD = dotenv_values().get('DB_PASSWORD')
DB_TABLE_EMPLOYEES_BOK = dotenv_values().get('DB_TABLE_EMPLOYEES_BOK')
DB_TABLE_COURSES_JOB_POSITION_MAP = dotenv_values().get('DB_TABLE_COURSES_JOB_POSITION_MAP')
DB_TABLE_CERTIFICATIONS_COURSES_MAP = dotenv_values().get('DB_TABLE_CERTIFICATIONS_COURSES_MAP')
DB_TABLE_SYSTEM_CONFIGURATION = dotenv_values().get('DB_TABLE_SYSTEM_CONFIGURATION')

DB_TABLE_EMPLOYEES_COURSES_MAP = dotenv_values().get('DB_TABLE_EMPLOYEES_COURSES_MAP')

# https://learn.microsoft.com/en-us/sql/sql-server/maximum-capacity-specifications-for-sql-server?view=sql-server-ver16
MAX_PARAMETERS_BY_QUERY = 2000

conn = pyodbc.connect(
    f'DRIVER={DB_DRIVER};SERVER={DB_SERVER};'
    f'DATABASE={DB_NAME};UID={DB_USERNAME};'
    f'PWD={DB_PASSWORD};Encrypt=no',
    autocommit=False
)

cursor = conn.cursor()

def retrieve_result_where():
    """."""
    get_property_info = '''
    SELECT Property, Tag, Value FROM [TMS].[dbo].[SystemConfigurations]
    '''
    result = cursor.execute(get_property_info).fetchall()

    return result

def new_employees_search(new_employees_id: list[int]):
    """."""
    #*** Extraction from JobPositionId, CertificationsId, CourseId***#
    try:
        query_select = f'''
            SELECT
            -- Employees
            e.[Id] , e.[JobPositionId],
            -- CoursesJobPositionsMap
            c_jp_m.[CourseId], c_jp_m.[Enabled], c_jp_m.[Mandatory]
            --CertificationsCoursesMap
            ,c_c_m.[CertificationId]
            FROM {DB_NAME}.{DB_TABLE_EMPLOYEES_BOK} AS e
            LEFT JOIN {DB_NAME}.{DB_TABLE_COURSES_JOB_POSITION_MAP} AS c_jp_m ON e.[JobPositionId] = c_jp_m.[JobPositionId]
            LEFT JOIN {DB_NAME}.{DB_TABLE_CERTIFICATIONS_COURSES_MAP} AS c_c_m ON c_c_m.[CourseId] = c_jp_m.[CourseId]
            WHERE e.[Id] IN ({','.join(str(value) for value in new_employees_id)});
            '''
        result = cursor.execute(query_select).fetchall()

        #Insert courses  with data select#
        for row1 in result:
            employees_id = row1[0]
            course_id = row1[2]
            mandatory = row1[4]
            certification_id = row1[5]

            if course_id is not None:
                if certification_id is None:
                    certification_id = 'NULL'
                if mandatory is False:
                    mandatory = 0
                if mandatory:
                    mandatory = 1

                cursor.execute(f'''
                  INSERT INTO {DB_NAME}.{DB_TABLE_EMPLOYEES_COURSES_MAP} (
                        [EmployeeId], [CourseId], [CertificationId], [Score], [Completed],
                      [TargetDate], [ExpirationDate], [ObjectMiscInfoId], [Mandatory],
                      [Enabled], [Created], [Updated], [UpdatedBy], [Type]
                      )
                    VALUES (
                      {employees_id}, {course_id}, {certification_id}, 0, 0, NULL, NULL, NULL,
                        {mandatory}, 1, GETDATE(), GETDATE(), 1, 'PRES');
                    ''')
                cursor.commit()
            else:
            #	log("No courses insert for this employee")
                print("No courses insert for this employee")
    except ValueError as e:
        #log(f"Error Value {e}")
        print(f"Error Value {e}")
    except TimeoutError as e:
        #log(f"Error Value {e}")
        print(f"Error Value {e}")
    except ConnectionError as e:
        #log(f"Error Value {e}")
        print(f"Error Value {e}")

def update_employees_courses(update_employees_id: list[int]):
    """update wich an employee up his/her job position add new course depend positionjob"""
    try:
        #***Change all courses ENABLE***#
        cursor.execute(f'''UPDATE [TMS].[dbo].[zDeVEmployeesCoursesMap]
        SET Enabled = 0 WHERE EmployeeId IN ({', '.join(str(value) for value in update_employees_id)});''')
        cursor.commit()

        query_select = f'''
        SELECT
        -- Employees
        e.[Id] , e.[JobPositionId],

        -- CoursesJobPositionsMap
        c_jp_m.[CourseId], c_jp_m.[Enabled], c_jp_m.[Mandatory]

        --CertificationsCoursesMap
        ,c_c_m.[CertificationId]

        FROM [TMS].[dbo].[Employees] AS e
        LEFT JOIN {DB_NAME}.{DB_TABLE_COURSES_JOB_POSITION_MAP} AS c_jp_m ON e.[JobPositionId] = c_jp_m.[JobPositionId]
        LEFT JOIN {DB_NAME}.{DB_TABLE_CERTIFICATIONS_COURSES_MAP} AS c_c_m ON c_c_m.[CourseId] = c_jp_m.[CourseId]
        WHERE e.[Id] IN ({','.join(str(value) for value in update_employees_id)});
        '''
        result = cursor.execute(query_select).fetchall()
        result_actual_course = cursor.execute(f'''
        SELECT EmployeeId, CourseId FROM {DB_NAME}.{DB_TABLE_EMPLOYEES_COURSES_MAP} WHERE EmployeeId IN ({','.join(str(value) for value in update_employees_id)});
        ''').fetchall()

        data = []
        for row1 in result_actual_course:
            data.append(row1[1])
        ##If detected course repeted update Enable = 0 if course are not add new course with his/her job position
        for row2 in result:
            if row2[2] in data:
                employee_update_id = row2[0]
                column_course_id = row2[2]
                cursor.execute(f'''UPDATE {DB_NAME}.{DB_TABLE_EMPLOYEES_COURSES_MAP}
                                    SET Enabled = 1 WHERE EmployeeId = {employee_update_id}
                                    AND CourseId = {column_course_id}''')
                cursor.commit()
            else:
                employees_id = row2[0]
                course_id = row2[2]
                mandatory = row2[4]
                certification_id = row2[5]
                if course_id is not None:
                    if certification_id is None:
                        certification_id = 'NULL'
                    if mandatory is False:
                        mandatory = 0
                    if mandatory:
                        mandatory = 1
                    cursor.execute(f'''
                    INSERT INTO {DB_NAME}.{DB_TABLE_EMPLOYEES_COURSES_MAP} (
                    [EmployeeId], [CourseId], [CertificationId], [Score], [Completed],
                    [TargetDate], [ExpirationDate], [ObjectMiscInfoId], [Mandatory],
                    [Enabled], [Created], [Updated], [UpdatedBy], [Type]
                    )
                    VALUES (
                    {employees_id}, {course_id}, {certification_id}, 0, 0, NULL, NULL, NULL,
                    {mandatory}, 1, GETDATE(), GETDATE(), 1, 'PRES');
                    ''')
                    cursor.commit()
                else:
                    #log("No courses insert for this employee")
                    print("No courses insert for this employee")
    except ValueError as e:
        #log(f"Error Value {e}")
        print(f"Error Value {e}")
    except TimeoutError as e:
        #log(f"Error Value {e}")
        print(f"Error Value {e}")
    except ConnectionError as e:
        #log(f"Error Value {e}")
        print(f"Error Value {e}")

def close_db_connection():
    """Close the connection at the end of the script."""
    conn.close()
