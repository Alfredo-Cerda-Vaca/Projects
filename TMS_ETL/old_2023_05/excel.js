const xlsxFile = require('read-excel-file/node');
var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var TYPES = require('tedious').TYPES;
var async = require('async');
var fs = require('fs-extra');
const fsFile = require("fs");
var nodemailer = require('nodemailer');
const { send } = require('process');

var filePath = '\\\\192.168.10.2\\data\\Share\\Manpower\\';
var newPath = 'D:\\Applications\\ETL\\TMSEmployees\\Processed\\';
/**
 * 1. Inactivas usuarios
 * 2. Verificas que existan
 * 2.1 Si no existen los insertas
 * 2.2 Si existen los activas y actualizas supervisor
 * 3. Si no existe supervisor lo pones en 0 y continuas con el proceso
 * 4. Se lee archivo de nuevo pero ahora solo se atacan los de supervisor con 0
 *  Connect SQL Server */

var transporter = nodemailer.createTransport({
	host: "smtp.office365.com",
	port: 587,
	secure: false,
	auth: {
		user: 's-mx-webapps@usiglobal.com',
		pass: 'Webapps01'
	}
});

var mailOptions = {
	from: 's-mx-webapps@usiglobal.com',
	to: 's-mx-webapps@usiglobal.com',
	subject: 'ETL TMS'
};


var config = {
	server: 'webservice.mx_usi.com.mx',  //update me
	authentication: {
		type: 'default',
		options: {
			userName: 'sa', //update me
			password: 'ZxCv123'  //update me
		}
	},
	options: {
		// If you are on Microsoft Azure, you need encryption:
		encrypt: false,
		database: 'TMS',  //update me,
		requestTimeout: 0,
		cryptoCredentialsDetails: {
			minVersion: 'TLSv1'
		}
	}
};

setTimeout(function () {
	generateLogTxt("Comienza funcion");
	//sendMailLog("Start ETL Execution");

	var connection = new Connection(config);

	function readFile(callback) {
		generateLogTxt("Lee el file");
		var listData = [];
		xlsxFile(filePath + 'HC.xlsx').then((rows) => {
			for (i in rows) {
				if (i == 0)
					continue;

				var data = [
					rows[i][0] == null ? "" : rows[i][0].toString(),
					rows[i][1] == null ? "" : rows[i][1].toString(),
					rows[i][2] == null ? "" : rows[i][2].toString(),
					rows[i][3] == null ? "" : rows[i][3].toString(),
					rows[i][4] == null ? "" : rows[i][4].toString(),
					rows[i][5] == null ? "" : rows[i][5].toString(),
					rows[i][6] == null ? "" : rows[i][6].toString(),
					rows[i][7].toString(),
					rows[i][8] == null ? "" : rows[i][8].toString(),
					rows[i][9] == null ? "" : rows[i][9].toString(),
					rows[i][10] == null ? "" : rows[i][10].toString(),
					rows[i][11] == null ? "" : rows[i][11].toString(),
					rows[i][12].toString(),
					rows[i][13] == null ? "" : rows[i][13].toString(),
					rows[i][14] == null ? "" : rows[i][14].toString(),
					rows[i][15] == null ? "" : rows[i][15].toString(),
					rows[i][16] == null ? "" : rows[i][16].toString(),
					rows[i][17] == null ? "" : rows[i][17].toString()
				]
				listData.push(data);
			}

			callback(null, listData);
		});
	};

	function dbInsert(employeeList, callback) {
		if (employeeList.length == 0) {
			generateLogTxt("employeeList.length == 0");
			callback(null, false);
		} else
			generateLogTxt("employeeList.length: " + employeeList.length.toString());

		var table = {
			columns: [
				{ name: '[CLAVE]', type: TYPES.VarChar },
				{ name: '[NOMBRE COMPLETO]', type: TYPES.VarChar },
				{ name: '[DESC. PUESTO]', type: TYPES.VarChar },
				{ name: '[DESC. DEPTO]', type: TYPES.VarChar },
				{ name: '[PROYECTO]', type: TYPES.VarChar },
				{ name: '[PERSONAL_DL1]', type: TYPES.VarChar },
				{ name: '[P/E]', type: TYPES.VarChar },
				{ name: '[F. INGRESO]', type: TYPES.DateTime },
				{ name: '[TURNO]', type: TYPES.VarChar },
				{ name: '[ESC.]', type: TYPES.VarChar },
				{ name: '[CURP]', type: TYPES.VarChar },
				{ name: '[SEXO]', type: TYPES.VarChar },
				{ name: '[FECHA NAC.]', type: TYPES.DateTime },
				{ name: '[ESCUELA]', type: TYPES.VarChar },
				{ name: '[GRADO]', type: TYPES.VarChar },
				{ name: '[PROFESSIONAL CAT CODE]', type: TYPES.VarChar },
				{ name: '[COST CENTER]', type: TYPES.VarChar },
				{ name: '[SUPERVISOR]', type: TYPES.VarChar }
			],
			rows: employeeList
		};

		var request = new Request("LoadEmployees", function (err, rowCount, rows) {
			if (err) {
				if (undefined == rows || undefined == rowCount) {
					generateLogTxt("Load Employees: " + err);
					sendMailLog("Error: Load Employees - " + err.message);
				} else {
					generateLogTxt("Load Employees: " + err + " - " + rows[rowCount]);
					sendMailLog("Error: Load Employees - " + err.message + " - " + rowCount.toString());
				}

			}
			else {
				callback(null, true);
			}
		});

		request.addParameter('excelFile', TYPES.TVP, table);

		request.on("requestCompleted", function (rowCount, more) {
			connection.close();
			generateLogTxt("Connection closed");
		});

		connection.callProcedure(request);
	}

	function Complete(err, result) {
		generateLogTxt("Move File");
		if (err) {
			generateLogTxt("Error moving file: " + err.message);
			sendMailLog("Error moving file: " + err.message);
		} else {
			var date = new Date()

			var day = date.getDate()
			var month = date.getMonth() + 1
			var year = date.getFullYear()

			fs.move(filePath + 'HC.xlsx', newPath + 'HC' + `${day}-${month}-${year}` + '.xlsx', err => {
				if (err) {
					generateLogTxt("Error moving file: " + err.message);
					sendMailLog("Error moving file: " + err.message);
					return false;
				} else {
					generateLogTxt("Success!");
					//sendMailLog("Success!");
				}
			});

		}
	}

	if (fsFile.existsSync(filePath + 'HC.xlsx')) {
		generateLogTxt("Entra conectar");
		connection.on('connect', function (err) {
			// If no error, then good to proceed.
			if (err) {
				generateLogTxt("Error: " + err.message);
				sendMailLog("Error: " + err.message);
			} else {
				generateLogTxt("Connected");
				async.waterfall([
					readFile,
					dbInsert
				], Complete)
			}
		});
		connection.connect();
	}
	else {
		generateLogTxt("No existe file: " + filePath + 'HC.xlsx');
		sendMailLog("Error: File not found: " + filePath + "HC.xlsx");
	}
}, 600000000);
/*600000000*/

function generateLogTxt(content) {
	console.error(content);
	var date = new Date();
	fsFile.writeFile('D:\\Applications\\ETL\\TMS_ETL\\log.txt', date.toLocaleString() + " - " + content + "\n", { flag: 'a+' }, err => {
		if (err) {
			console.error(err);
		}
	});
}

function sendMailLog(content) {
	setTimeout(() => {
		mailOptions.html = "<span style='font-size: 12px; font-weight: bold;'>" + content + "</span>";

		transporter.sendMail(mailOptions, function (error, info) {
			if (error) {
				generateLogTxt(error.message);
			}
		});
	}, 50000);

}