DROP TABLE IF EXISTS FactBatchInfo;
DROP TABLE IF EXISTS FactFileInfo;
DROP TABLE IF EXISTS FactFileStatus;
DROP TABLE IF EXISTS FactBatchStatus;

create table FactFileStatus(
ID INT NOT NULL auto_increment,
BatchID varchar(40) not null,
FileID varchar(100) not null,
StatusID int not null,
ProcessStartDtTm datetime not null,
ProcessEndDtTm datetime,
LastModifiedDtTm datetime NOT NULL,
LastModifiedBy varchar(20) not null,
GcsUri Varchar(100) not null,
unique(ID),
primary key (FileID),
CONSTRAINT FK_StatusID_FactFileStatus FOREIGN KEY(StatusID) REFERENCES DimStatus(StatusID));

create table FactFileInfo(
ID INT NOT NULL auto_increment,
BatchID varchar(40) not null,
FileID varchar(100) not null,
ProcessStartDtTm datetime not null,
ProcessEndDtTm datetime not null,
StatusId Int not null,
LastModifiedDtTm datetime not null,
LastModifiedBy varchar(20) not null,
Description varchar(100),
Primary key(ID),
Constraint FK_StatusID_FactFileInfo FOREIGN KEY(StatusId) References DimStatus(StatusID));

create table FactBatchStatus(
ID INT NOT NULL auto_increment,
BatchID varchar(40) not null,
StatusID int not null,
BatchStartDtTm datetime not null,
BatchEndDtTm datetime,
TotalFiles int not null,
LastModifiedDtTm datetime not null,
LastModifiedBy varchar(20) not null,
GcsUri Varchar(100) not null,
UNIQUE (ID),
primary key(BatchID),
Constraint FK_StatusID_FactBatchStatus foreign key(StatusID) references DimStatus(StatusID));


create table FactBatchInfo(
ID INT NOT NULL auto_increment,
BatchID varchar(40) not null,
ProcessStartDtTm datetime not null,
ProcessEndDtTm datetime,
StatusID int not null,
LastModifiedDtTm datetime not null,
LastModifiedBy varchar(20) not null,
Description varchar(100),
primary key(ID),
Constraint FK_StatusID_FactBatchInfo foreign key(StatusID) references DimStatus(StatusID));

ALTER TABLE FactBatchInfo
ADD constraint FK_BatchID_FactBatchInfo
FOREIGN KEY (BatchID) REFERENCES FactBatchStatus(BatchID);

ALTER TABLE FactFileInfo
ADD constraint FK_BatchID_FactFileInfo
FOREIGN KEY (BatchID) REFERENCES FactBatchStatus(BatchID);	

ALTER TABLE FactFileInfo
ADD constraint FK_FileID_FactFileInfo
FOREIGN KEY (FileID) REFERENCES FactFileStatus(FileID);	

ALTER TABLE FactFileStatus
ADD constraint FK_BatchID_FactFileStatus
FOREIGN KEY (BatchID) REFERENCES FactBatchStatus(BatchID);	
