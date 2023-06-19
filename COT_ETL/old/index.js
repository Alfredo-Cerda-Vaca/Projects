
var oracledb = require('oracledb');
var sql = require('mssql');
var moment = require('moment');
var fs = require('fs');
const { Console } = require('console');
oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
require('dotenv').config();


//Global variables
var re = /[/]/gi;
var ScheduledTrigger;
var logDate = new Date().toLocaleDateString().replace(re, '-');
var logStdout = process.stdout;
const output = fs.createWriteStream('./logs/migrationService-' + logDate + '.log', { flags: 'a' });
const logger = new console.Console(output);


//Connection
const config = {
    user: process.env.USERSQLS,
    password: process.env.PASSWORDSQLS,
    server: process.env.HOSTSQLS, // You can use 'localhost\\instance' to connect to named instance
    database: process.env.DBSQLS,
    "dialectOptions": {
        options: { "requestTimeout": 600000 }
    },
    options: {
        'requestTimeout': 600000,
        cryptoCredentialsDetails: {
            minVersion: 'TLSv1'
        },
        encrypt: false,
        trustServerCertificate: false
    }
}

/**
 * Get results from Oracle
 * @param {*} queryString 
 * @returns 
 */
async function getResultFromOracle(queryString) {
    var connection;
    var response = {
        success: false,
        data: []
    };
    try {
        connection = await oracledb.getConnection({

            user: process.env.USERORC,
            password: process.env.PASSWORDORC,
            connectString: `${process.env.HOSTORC}:${process.env.PORTORC}/${process.env.SERVICE_NAMEORC}`
        });

        let queryResult = await connection.execute(queryString);
        response = {
            success: true,
            data: queryResult?.rows
        }
    } catch (err) {
        console.log(err);
    } finally {
        if (connection) {
            try {
                await connection.close();
            } catch (err) {
                console.log("Error");
            }
        }
    }
    return response;
}
/**
 * 
 * 
 */
async function getRangeDates() {
    try {
        let pool = await sql.connect(config);
        let results = await pool.request().query(
            `SELECT top 1 sh.datestart as PrevStart, sh.dateend as DateStart, DATEADD(HOUR,sc.[HoursToAdd],dateend) as DateEnd,
            CASE WHEN sh.datestart <= DATEADD(HOUR,-24,getdate()) THEN '1' else '0' end as Run, DATEADD(HOUR,-24,getdate())
            FROM [eDashboard_FPY].[dbo].[GeneralScheduledHistory] sh (NOLOCK) 
            INNER JOIN [eDashboard_FPY].[dbo].[GeneralScheduledConfiguration] sc (NOLOCK) ON sh.ScheduledTaskName = sc.ScheduledTaskName
            WHERE sh.ScheduledTaskName ='USI_COT_ETL' AND Enabled = 1 AND [Status] = 'Success'
            ORDER BY sh.id DESC`
        )
        return results.recordset;

    } catch (error) {
        log(`${error}`);
    }
}

async function insertHistoryScheduled(startDate, endDate, msg) {
    //console.log(id, finalDate);
    //console.log(`'${moment(finalDate).format('YYYY-MM-DD HH:mm')}'`, finalDate);
    try {
        let queryString = `
        INSERT INTO [dbo].[GeneralScheduledHistory] ([ScheduledTaskName],[LastExecution],[DateStart],[DateEnd],[Status])
     VALUES ('USI_COT_ETL',GETDATE(),'${startDate}','${endDate}','${msg}')
        `
        let pool = await sql.connect(config);
        let results = await pool.request().query(queryString);

    } catch (error) {
        console.log('insertHistoryScheduled', `${error}`)
        log(`${error}`);
    }
}

/**
 * Main Process
 */
async function mainProcess() {
    console.log('Inicio')
    var origin1;
    try {

        ScheduledTrigger = await getRangeDates();
        console.log(ScheduledTrigger)
        if (ScheduledTrigger.length > 0) {

            console.log('Inicio-', new Date());
            const jsStartDatetime = ScheduledTrigger[0].DateStart;
            const jsEndDatetime = ScheduledTrigger[0].DateEnd;
            const jobRun = ScheduledTrigger[0].Run;

            const sqlStartDatetime = jsStartDatetime.toISOString().substring(0, 19).replace('T', ' ');
            const sqlEndDatetime = jsEndDatetime.toISOString().substring(0, 19).replace('T', ' ');


            let statemet1;

            console.log('initDate vs endDate', jsStartDatetime, jsEndDatetime, sqlStartDatetime, sqlEndDatetime);
            if (jsStartDatetime != jsEndDatetime) {
                //if(1 == 1){				
                statemet1 = `
				SELECT art.RECORD_ID,art.BU,art.DEPT,art.LINE_NAME,art.MODEL_NAME, 
                CASE WHEN m2.CUSTOMER IS NULL THEN 'NULL' ELSE m2.CUSTOMER END AS CUSTOMER, 
                CASE WHEN m2.PROJECT_NAME IS NULL THEN 'NULL' ELSE m2.PROJECT_NAME END AS PROJECT_NAME,
                art.SHIFT_ID,art.EXCEPT_ID,art.ACTIVATE_TYPE,art.ACTIVATE_EMP_NO,art.CONFIRM_EMP_NO,art.START_TIME,art.END_TIME,art.STATUS,
                TO_CHAR(art.TOTAL_SUPPORT) AS TOTAL_SUPPORT,
                    TO_CHAR(art.TOTAL_TIME) AS TOTAL_TIME,
                        TO_CHAR(art.TOTAL_DAY) AS TOTAL_DAY,
                art.REMARK,art.MACHINE,art.BD_SIDE,art.SUPPORT_DEPT,art.MO_NUMBER,cect.EXCEPT_NAME,cect.EXCEPT_DESC,cect.EXCEPT_OWNER
                FROM SMTB.R_ACTIVATION_RECORD_T art
                LEFT JOIN smt.C_EXCEPT_CODE_T cect ON (art.EXCEPT_ID =CECT.EXCEPT_ID)
                LEFT JOIN SFIS1.C_MODEL_DESC2_t m2 ON (art.MODEL_NAME = m2.MODEL_NAME)
                WHERE START_TIME BETWEEN TO_DATE ('${sqlStartDatetime}', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE ('${sqlEndDatetime}', 'YYYY-MM-DD HH24:MI:SS') 
                AND art.STATUS = 'CLOSE'
				`
            } else {
                statemet1 = `
				SELECT art.RECORD_ID,art.BU,art.DEPT,art.LINE_NAME,art.MODEL_NAME, m2.CUSTOMER, m2.PROJECT_NAME,art.SHIFT_ID,art.EXCEPT_ID,art.ACTIVATE_TYPE,art.ACTIVATE_EMP_NO,art.CONFIRM_EMP_NO,art.START_TIME,art.END_TIME,art.STATUS,art.TOTAL_SUPPORT,art.TOTAL_TIME,art.TOTAL_DAY,art.REMARK,art.MACHINE,art.BD_SIDE,art.SUPPORT_DEPT,art.MO_NUMBER,cect.EXCEPT_NAME,cect.EXCEPT_DESC,cect.EXCEPT_OWNER
                FROM SMTB.R_ACTIVATION_RECORD_T art
                LEFT JOIN smt.C_EXCEPT_CODE_T cect ON (art.EXCEPT_ID =CECT.EXCEPT_ID)
                LEFT JOIN SFIS1.C_MODEL_DESC2_t m2 ON (art.MODEL_NAME = m2.MODEL_NAME)
                WHERE START_TIME BETWEEN TO_DATE ('${sqlStartDatetime}', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE ('${sqlEndDatetime}', 'YYYY-MM-DD HH24:MI:SS') 
				`
            }

            console.log(statemet1);

            //console.log('${moment(ScheduledTrigger[0].InitHour).format('YYYYMMDD')}');
            //console.log((ScheduledTrigger[0].EndHour).format('hh'));
            console.log('-----1.- Conectando a Oracle COT (origin 1)......');
            origin1 = await getResultFromOracle(statemet1);
            console.log('-----2.- Trae resultados COT');

            //Send results to sql
            if (jobRun == '1') {


                if (null != origin1.data && origin1.data.length > 0) {
                    console.log('-----3.- Cantidad de datos de COT: ', origin1.data.length);
                    await insertSegment(origin1.data, sqlStartDatetime, sqlEndDatetime);
                } else {
                    //await insertSegment2(origin2.data);
                    insertHistoryScheduled(sqlStartDatetime, sqlEndDatetime, 'Success');
                    console.error('---NO COT Info---')
                }
            } else {
                insertHistoryScheduled(sqlStartDatetime, sqlEndDatetime, 'No Run');
                console.error('---NO COT Info---')
            }
            //When results inserted succesful register history for avoid duplicate data


        } else {
            console.log('No se encontro configuracion de ultima ejecucion');
        }

    } catch (error) {
        log(`${error}`);
    }
}

/**
 * Insert second query in sql server
 * @param {*} data 
 */
async function insertSegment(data, sqlStartDatetime, sqlEndDatetime) {
    //console.log(data);

    try {
        console.log('-----4.- Iniciando insercion de COT......')

        const table = new sql.Table('TmpCOT') // or temporary table, e.g. #temptable
        table.create = true
        table.columns.add('RECORD_ID', sql.VarChar(250), { nullable: true });
        table.columns.add('BU', sql.VarChar(250), { nullable: true });
        table.columns.add('DEPT', sql.VarChar(250), { nullable: true });
        table.columns.add('LINE_NAME', sql.VarChar(250), { nullable: true });
        table.columns.add('MODEL_NAME', sql.VarChar(250), { nullable: true });
        table.columns.add('CUSTOMER', sql.VarChar(250), { nullable: true });
        table.columns.add('PROJECT_NAME', sql.VarChar(250), { nullable: true });
        table.columns.add('SHIFT_ID', sql.VarChar(250), { nullable: true });
        table.columns.add('EXCEPT_ID', sql.VarChar(250), { nullable: true });
        table.columns.add('ACTIVATE_TYPE', sql.VarChar(250), { nullable: true });
        table.columns.add('ACTIVATE_EMP_NO', sql.VarChar(250), { nullable: true });
        table.columns.add('CONFIRM_EMP_NO', sql.VarChar(250), { nullable: true });
        table.columns.add('START_TIME', sql.DateTime, { nullable: true });
        table.columns.add('END_TIME', sql.DateTime, { nullable: true });
        table.columns.add('STATUS', sql.VarChar(250), { nullable: true });
        table.columns.add('TOTAL_SUPPORT', sql.VarChar(250), { nullable: true });
        table.columns.add('TOTAL_TIME', sql.VarChar(250), { nullable: true });
        table.columns.add('TOTAL_DAY', sql.VarChar(250), { nullable: true });
        table.columns.add('REMARK', sql.VarChar(250), { nullable: true });
        table.columns.add('MACHINE', sql.VarChar(250), { nullable: true });
        table.columns.add('BD_SIDE', sql.VarChar(250), { nullable: true });
        table.columns.add('SUPPORT_DEPT', sql.VarChar(250), { nullable: true });
        table.columns.add('MO_NUMBER', sql.VarChar(250), { nullable: true });
        table.columns.add('EXCEPT_NAME', sql.VarChar(250), { nullable: true });
        table.columns.add('EXCEPT_DESC', sql.VarChar(250), { nullable: true });
        table.columns.add('EXCEPT_OWNER', sql.VarChar(250), { nullable: true });
        //Formating query
        var data3 = data.map(result => {

            var dataAray = [];
	    var sdate = new Date(result.START_TIME.toISOString().slice(0,19) + 'Z');
            var edate = new Date(result.END_TIME.toISOString().slice(0,19) + 'Z');

            var utcSDate = new Date(sdate.getTime() + sdate.getTimezoneOffset()*60000);
            var utcEDate = new Date(edate.getTime() + edate .getTimezoneOffset()*60000);


            table.rows.add(
                result.RECORD_ID,
                result.BU,
                result.DEPT,
                result.LINE_NAME,
                result.MODEL_NAME,
                result.CUSTOMER,
                result.PROJECT_NAME,
                result.SHIFT_ID,
                result.EXCEPT_ID,
                result.ACTIVATE_TYPE,
                result.ACTIVATE_EMP_NO,
                result.CONFIRM_EMP_NO,
                //result.START_TIME,
                moment(utcSDate).format('YYYY-MM-DD HH:mm:ss'),
                //result.END_TIME,
                moment(utcEDate).format('YYYY-MM-DD HH:mm:ss'),
                result.STATUS,
                result.TOTAL_SUPPORT,
                result.TOTAL_TIME,
                result.TOTAL_DAY,
                result.REMARK,
                result.MACHINE,
                result.BD_SIDE,
                result.SUPPORT_DEPT,
                result.MO_NUMBER,
                result.EXCEPT_NAME,
                result.EXCEPT_DESC,
                result.EXCEPT_OWNER
            )

            return dataAray;
        })


        var dataInsert = `
        INSERT INTO [dbo].[COT]
            ([RECORD_ID]
                ,[BU]
                ,[DEPT]
                ,[LINE_NAME]
                ,[MODEL_NAME]
                ,[CUSTOMER]
                ,[PROJECT_NAME]
                ,[SHIFT_ID]
                ,[EXCEPT_ID]
                ,[ACTIVATE_TYPE]
                ,[ACTIVATE_EMP_NO]
                ,[CONFIRM_EMP_NO]
                ,[START_TIME]
                ,[END_TIME]
                ,[STATUS]
                ,[TOTAL_SUPPORT]
                ,[TOTAL_TIME]
                ,[TOTAL_DAY]
                ,[REMARK]
                ,[MACHINE]
                ,[BD_SIDE]
                ,[SUPPORT_DEPT]
                ,[MO_NUMBER]
                ,[EXCEPT_NAME]
                ,[EXCEPT_DESC]
                ,[EXCEPT_OWNER]
                ,[CREATE_DATE])
        SELECT
        t.[RECORD_ID]
        ,t.[BU]
        ,t.[DEPT]
        ,t.[LINE_NAME]
        ,t.[MODEL_NAME]
        ,t.[CUSTOMER]
        ,t.[PROJECT_NAME]
        ,t.[SHIFT_ID]
        ,t.[EXCEPT_ID]
        ,t.[ACTIVATE_TYPE]
        ,t.[ACTIVATE_EMP_NO]
        ,t.[CONFIRM_EMP_NO]
        ,t.[START_TIME]
        ,t.[END_TIME]
        ,t.[STATUS]
        ,t.[TOTAL_SUPPORT]
        ,t.[TOTAL_TIME]
        ,t.[TOTAL_DAY]
        ,t.[REMARK]
        ,t.[MACHINE]
        ,t.[BD_SIDE]
        ,t.[SUPPORT_DEPT]
        ,t.[MO_NUMBER]
        ,t.[EXCEPT_NAME]
        ,t.[EXCEPT_DESC]
        ,t.[EXCEPT_OWNER]
        ,GETDATE()
        FROM TmpCOT t
        `

        var tempDatadelete = `DELETE FROM TmpCOT`
        //console.log(table.rows)

        let pool = await sql.connect(config)
        let result1 = await pool.request()
        result1.stream = true

        result1.bulk(table, (err, result) => {
            // ... error checks
            console.log(result);
            console.log(err);
        })

        console.log('-----5.- Insertando TEM COT------')
        let result2 = await pool.request()


        let result3 = await pool.request()



        let rowsToProcess = [];
        result1.on('rowsaffected', rowCount => {
            rowsToProcess.push(row);
            if (rowsToProcess.length >= 1000) {
                request.pause();
                processRows();
            }
            console.log('RowAfected: ', rowCount)
            // Emitted for each `INSERT`, `UPDATE` or `DELETE` statement
            // Requires NOCOUNT to be OFF (default)
        })

        function processRows() {
            // process rows
            rowsToProcess = [];
            result1.resume();
        }

        var error = false;
        result1.on('error', err => {
            console.log('-----Insert error: ', err)
            error = true;
        })

        result1.on('done', result => {
            // Always emitted as the last one
            
            if (!error) {
                console.log('-----6.- Insert TMP COT completado: ', result)
                result2.query(dataInsert).then(result => {
                    console.log('-----7.- COT Insertada en tabla definitiva: ', result.rowsAffected)
                    result3.query(tempDatadelete).then(result => {
                        console.log('-----8.- COT Datos temporales Eliminado: ', result.rowsAffected)
                        insertHistoryScheduled(sqlStartDatetime, sqlEndDatetime, 'Success');
                    })
                })
            } else {
                insertHistoryScheduled(sqlStartDatetime, sqlEndDatetime, 'Error Nulls');
            }


        })

        result2.on('error', err => {
            console.log('Copy Table error: ', err)
        })

        result2.on('done', result => {
            // Always emitted as the last one
            console.log('Delete done: ', result)

        })

    } catch (error) {
        log(`${error}`);
    }
}

function log(message) {
    const currentTimeStamp = new Date().toLocaleDateString('en-US', { year: 'numeric', month: '2-digit', day: '2-digit', hour12: false, minute: '2-digit', second: '2-digit' });
    const logPrefix = currentTimeStamp + " -|-";
    let logmsg = logPrefix + message;
    logger.log(logmsg);
    logStdout.write(logmsg + "\n");
}



mainProcess();