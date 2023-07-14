# Indexes for tables in production
CREATE INDEX IX_EmployeeProperties_EmployeeId_Tag
ON [TMS].[dbo].[EmployeeProperties] ([EmployeeId], [Tag]);

CREATE INDEX IX_EmployeeProperties_EmployeeId
ON [TMS].[dbo].[EmployeeProperties] ([EmployeeId]);

CREATE INDEX IX_Departments_Code
ON [TMS].[dbo].[Departments] ([Code]);

CREATE INDEX IX_JobPositions_Name
ON [TMS].[dbo].[JobPositions] ([Name]);

# Indexes to add
UPDATE {DB_NAME}.{DB_TABLE_EMPLOYEE_PROPERTIES}
[EmployeeId] = ? AND [Tag] = ?;

CREATE INDEX IX_zDevEmployeeProperties_EmployeeId_Tag
ON [TMS].[dbo].[zDevEmployeeProperties] ([EmployeeId], [Tag]);

CREATE INDEX IX_zDevEmployeeProperties_EmployeeId
ON [TMS].[dbo].[zDevEmployeeProperties] ([EmployeeId]);
--
SELECT [Id],[Code],[Alias],[Name],[Enabled],[Created],[Updated],[UpdatedBy]
	FROM {DB_NAME}.{DB_TABLE_DEPARTMENTS}
	WHERE [Code] = ?

CREATE INDEX IX_zDevDepartments_Code
ON [TMS].[dbo].[zDevDepartments] ([Code]);
--
SELECT [Id], [Code]
FROM {DB_NAME}.{DB_TABLE_JOB_POSITIONS}
WHERE [Name] = ?

CREATE INDEX IX_zDevJobPositions_Name
ON [TMS].[dbo].[zDevJobPositions] ([Name]);
--
UPDATE {DB_NAME}.{DB_TABLE_EMPLOYEES}
SET [Enabled] = '{status}'
WHERE [Code] IN
	({', '.join('?' for _ in current_employee_codes_tuple)});
--
SELECT * from sys.indexes
WHERE object_id = (SELECT object_id FROM sys.objects WHERE name = 'zDevEmployeeProperties')

SELECT * from sys.indexes
WHERE object_id = (SELECT object_id FROM sys.objects WHERE name = 'zDevDepartments')

DROP IX_zDevJobPositions_Name
ON TMS.dbo.zDevDepartments

# Create a table resembling the XLSX structure
CLAVE	NOMBRE COMPLETO	DESC. PUESTO	AREA	BU	PERSONAL_DL1	P/E	F. INGRESO	TURNO	ESC.	CURP	SEXO	FECHA NAC.	ESCUELA	GRADO	PROFESSIONAL CAT CODE	COST CENTER	SUPERVISOR


# Columns on the Source Excel File
CLAVE
NOMBRE COMPLETO
DESC. PUESTO
DESC. DEPTO
PROYECTO
PERSONAL_DL1
P/E
F. INGRESO
TURNO
ESC.
CURP
SEXO
FECHA NAC.
ESCUELA
GRADO	PROFESSIONAL
CAT CODE
COST CENTER
SUPERVISOR

# Mapping as DB_Fields
CLAVE -> CODE
NOMBRE COMPLETO	-> FULL_NAME
DESC. PUESTO -> JOB_POSITION
DESC. DEPTO	-> DEPARTMENT_DESCRIPTION
PROYECTO -> PROJECT_NAME
PERSONAL_DL1 -> PERSONAL_DL1
P/E -> P_E
F. INGRESO -> START_DATE
TURNO -> SHIFT
ESC. -> EDUCATION
CURP -> CURP
SEXO -> SEX
FECHA NAC. -> DOB
ESCUELA	-> SCHOOL
GRADO	-> DEGREE
PROFESSIONAL CAT CODE	-> PROFESSIONAL_CAT_CODE
COST CENTER -> COST_CENTER
SUPERVISOR -> SUPERVISOR

- Get the email from the Active Directory
- Connect Supervisor with SupervisorID

Employees
- JobPositions (DESC. PUESTO)
- Shifts (TURNO)
- SupervisorID (Employee ID - SUPERVISOR)
- ESCUELA (?)
- COST CENTER (?)

Departments

Table Users?

- Insertar JobPositions con prefijos de tipo CJP-0000
- Revisar el último e intentar inserción

## Info extra 9may2023
La de departamento si quisiera agregarlo, ahi checate que es lo que mas conviene, creo que si trenemos departamentos, si quisiera poder agregarlo en la DB

Tanto el de DESC, DEPTO  como el de Proyecto
La parte de ESC lo pudieramos agregar en una tabla que se llama EmployeeProperty
Properties

La parte de Proyecto es Customer y yo creo que para ese escenario si tendriamso que agregar la referencia 
AL empleado

Turno creo que el 4 si existe,  los 14, 15 son como continentales

Todo lo extra que no se usa me gustaria poder guardarlo en EmployeeProperty

El Supervisor es por la clave, si la clave no existye es porque es un chino, ahi se le pone 0
Que es como General

---

CREATE TABLE [dbo].[Employees](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[Code] [varchar](50) NOT NULL,
	[SAP_ID] [varchar](50) NULL,
	[FirstName] [varchar](50) NOT NULL,
	[LastName] [varchar](50) NOT NULL,
	[Email] [varchar](50) NULL,
	[Phone] [varchar](50) NULL,
	[CURP] [varchar](50) NULL,
	[SocialSecurityNumber] [varchar](50) NULL,
	[Source] [varchar](50) NULL,
	[JobPositionId] [bigint] NULL,
	[Shift] [varchar](50) NULL,
	[Supervisor] [varchar](150) NULL,
	[SupervisorId] [varchar](50) NULL,
	[StartDay] [datetime] NULL,
	[EndDate] [datetime] NULL,
	[Enabled] [bit] NULL,
	[Created] [datetime] NULL,
	[Updated] [datetime] NULL,
	[UpdatedBy] [int] NULL,
 CONSTRAINT [PK_Employees] PRIMARY KEY CLUSTERED
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY],
 CONSTRAINT [IX_Employees] UNIQUE NONCLUSTERED
(
	[Code] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

---

CREATE TABLE [dbo].[JobPositions](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[Code] [varchar](50) NOT NULL,
	[Name] [varchar](50) NOT NULL,
	[Enabled] [bit] NOT NULL,
	[Created] [datetime] NOT NULL,
	[Updated] [datetime] NOT NULL,
	[UpdatedBy] [int] NOT NULL,
 CONSTRAINT [PK_JobPositions] PRIMARY KEY CLUSTERED
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY],
 CONSTRAINT [IX_JobPositions] UNIQUE NONCLUSTERED
(
	[Code] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[JobPositions]  WITH CHECK ADD  CONSTRAINT [FK_JobPositions_Users] FOREIGN KEY([UpdatedBy])
REFERENCES [dbo].[Users] ([Id])
GO

ALTER TABLE [dbo].[JobPositions] CHECK CONSTRAINT [FK_JobPositions_Users]
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Unique Job Position Code' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'JobPositions', @level2type=N'CONSTRAINT',@level2name=N'IX_JobPositions'
GO

---

CREATE TABLE [dbo].[Shifts](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Name] [varchar](32) NOT NULL,
	[StartTime] [time](7) NOT NULL,
	[EndTime] [time](7) NOT NULL,
	[Hour] [int] NOT NULL,
	[Enabled] [bit] NOT NULL,
	[Updated] [datetime] NOT NULL,
	[UpdatedBy] [int] NOT NULL,
 CONSTRAINT [PK_Shifts] PRIMARY KEY CLUSTERED
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[Shifts] ADD  CONSTRAINT [DF_Shifts_Enabled]  DEFAULT ((1)) FOR [Enabled]
GO

ALTER TABLE [dbo].[Shifts] ADD  CONSTRAINT [DF_Shifts_Updated]  DEFAULT (getdate()) FOR [Updated]
GO

---

CREATE TABLE [dbo].[EmployeeProperties](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[EmployeeId] [bigint] NOT NULL,
	[PropertyGroup] [varchar](100) NULL,
	[Tag] [varchar](150) NOT NULL,
	[Value] [varchar](500) NOT NULL,
	[Enabled] [bit] NOT NULL,
	[Created] [datetime] NOT NULL,
	[Updated] [datetime] NOT NULL,
	[UpdatedBy] [int] NOT NULL,
 CONSTRAINT [PK_EmployeeProperties] PRIMARY KEY CLUSTERED
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[EmployeeProperties]  WITH CHECK ADD  CONSTRAINT [FK_EmployeeProperties_Employees] FOREIGN KEY([EmployeeId])
REFERENCES [dbo].[Employees] ([Id])
GO

ALTER TABLE [dbo].[EmployeeProperties] CHECK CONSTRAINT [FK_EmployeeProperties_Employees]
GO

ALTER TABLE [dbo].[EmployeeProperties]  WITH CHECK ADD  CONSTRAINT [FK_EmployeeProperties_Users] FOREIGN KEY([UpdatedBy])
REFERENCES [dbo].[Users] ([Id])
GO

ALTER TABLE [dbo].[EmployeeProperties] CHECK CONSTRAINT [FK_EmployeeProperties_Users]
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Propiedades de empleado' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'EmployeeProperties', @level2type=N'COLUMN',@level2name=N'Enabled'
GO

---

--- Clear all the tables
DELETE FROM [TMS].[dbo].[zDevEmployeeProperties]
DELETE FROM [TMS].[dbo].[zDevEmployees]
DELETE FROM [TMS].[dbo].[zDevDepartments]
DELETE FROM [TMS].[dbo].[zDevJobPositions]

--- Get all employees with their Job Position
SELECT
	-- Employees
	e.[Id], e.[Code], e.[SAP_ID], e.[FirstName], e.[LastName], e.[Email],
	e.[Phone], e.[CURP], e.[SocialSecurityNumber], e.[Source],
	e.[JobPositionId], e.[Shift], e.[Supervisor], e.[SupervisorId],
	e.[StartDay], e.[EndDate], e.[Enabled],

	-- Job Positions
	j_p.[Id], j_p.[Code], j_p.[Name], j_p.[Enabled]
FROM [TMS].[dbo].[zDevEmployees] AS e

LEFT JOIN [TMS].[dbo].[zDevJobPositions] as j_p
ON e.[JobPositionId] = j_p.[Id]

--- Get all Employee Properties
SELECT [EmployeeId], [PropertyGroup], [Tag], [Value], [Enabled]

FROM [TMS].[dbo].[zDevEmployeeProperties]

--- Compare Dev and Production Tables - Employees
SELECT
e.[Id],
ze.[Id],
e.[Code],
ze.[Code],
e.[SAP_ID],
ze.[SAP_ID],
e.[FirstName],
ze.[FirstName],
e.[LastName],
ze.[LastName],
e.[Email],
ze.[Email],
e.[Phone],
ze.[Phone],
e.[CURP],
ze.[CURP],
e.[SocialSecurityNumber],
ze.[SocialSecurityNumber],
e.[Source],
ze.[Source],
e.[JobPositionId],
ze.[JobPositionId],
e.[Shift],
ze.[Shift],
e.[Supervisor],
ze.[Supervisor],
e.[SupervisorId],
ze.[SupervisorId],
e.[StartDay],
ze.[StartDay],
e.[EndDate],
ze.[EndDate],
e.[Enabled],
ze.[Enabled],
e.[Created],
ze.[Created],
e.[Updated],
ze.[Updated],
e.[UpdatedBy],
ze.[UpdatedBy]
FROM [TMS].[dbo].[Employees] AS e
INNER JOIN [TMS].[dbo].[zDevEmployees] AS ze ON e.Code = ze.Code;

# View [Supervisor_giro].[vista_empleado]
CLAVE, char(10)
NOMBRE_COMPLETO, varchar(92)
JUNTO, varchar(105)
NOMBRE, char(30)
APELLIDO_PATERNO, char(30)
APELLIDO_MATERNO, char(30)
INGRESO, datetime
PLANTA, datetime
FOTO, varchar(30)
PE, char(1)
RFC, char(13)
SEXO, char(1)
TIPO_NOM, char(4)
TIPO_PAGO, char(3)
TURNO, char(5)
DESCRIPCION_TURNO, char(30)
CTA_TARJETA, char(20)
PTU, bit
CUOTA_SINDICAL, bit
AFILIACION, char(11)
CTA_CHEQUES, char(10)
CURP, char(20)
JORNADA_REDUCIDA, smallint
SEMANA_REDUCIDA, smallint
CALENDARIO, char(2)
DESCRIPCION_CALENDARIO, char(30)
banco_cfdi, varchar(3)
regimen, varchar(2)
GLOBAL_EMPLOYEEID, varchar(90)
ESTADO_CIVIL, char(5)
ESCOLARIDAD, char(5)
EDAD, int
VIGENCIA, varchar(10)
FECHA_BAJA, datetime
PUESTO, varchar(10)
DESCRIPCION_PUESTO, char(30)
SUCURSAL, varchar(10)
DESCRIPCION_SUCURSAL, char(30)
DEPARTAMENTO, varchar(10)
DESCRIPCION_DEPTO, char(30)
PRESTACIONES, varchar(10)
PATRON, char(10)
DESCRIPCION_PATRON, char(50)
SALDO_VACACIONAL, float