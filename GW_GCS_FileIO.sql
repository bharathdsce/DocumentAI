create table FactFileStatus(
ID INT NOT NULL auto_increment,
BatchID varchar(40) not null,
FileID varchar(100) not null,
StatusID int not null,
ProcessStartDtTm datetime not null,
ProcessEndDtTm datetime not null,
LastModifiedDtTm datetime NOT NULL,
LastModifiedBy varchar(40) not null,
unique(ID),
primary key (FileID),
CONSTRAINT FK_StatusID_FactFileStatus FOREIGN KEY(StatusID) REFERENCES DimStatus(StatusID));

insert into FactFileStatus(batch_id, file_id, status_id, process_start_dttm, process_end_dttm)
values("test", "test_file", 2, sysdate(), sysdate())

drop table FactFileStatus

select * from FactFileStatus

insert into FactFileStatus(batch_id, file_id, status_id, process_start_dttm, process_end_dttm)
                values('87756713-bee8-11eb-aeb9-b0a460678da0', '0c1bae16-6f67-4afb-99be-fa3b829900c0.pdf', 2, '2021-05-27 18:09:07.924046', '2021-05-27 18:09:07.926046');
                
create table DimStatus(
StatusID INT NOT NULL,
StatusDesc Varchar(20) Not null,
LastModifiedDtTm datetime not null,
LastModifiedBy varchar(50) not null,
primary key(StatusID));

select * from DimStatus;
insert into DimStatus
values(1, "Created", sysdate(), "system")
, (2, "Process Started", sysdate(), "system")

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


truncate table FactBatchInfo;
truncate table FactFileInfo;
delete from FactFileStatus where 1=1;
delete from FactBatchStatus where 1=1;

select * from FactBatchInfo;
select * from FactFileInfo;
select * from FactFileStatus;
select * from FactBatchStatus;

drop table FactBatchInfo;
drop table FactFileInfo;
drop table FactFileStatus;
drop table FactBatchStatus;
