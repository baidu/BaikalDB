DROP TABLE IF EXISTS `Baikaltest`.`student`;
CREATE TABLE `Baikaltest`.`student` (
  `id` int(10) NOT NULL ,
  `name1` varchar(1024) NOT NULL ,
  `name2` varchar(1024) NOT NULL ,
  `age1` int(10) NOT NULL ,
  `age2` int(10) NOT NULL ,
  `class1` int(10) NOT NULL ,
  `class2` int(10) NOT NULL ,
  `address` varchar(1024) NOT NULL ,
  `height` int(10) NOT NULL ,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name1_key` (`name1`),
  UNIQUE KEY `name2_key` (`name2`),
  KEY `age1_key` (`age1`),
  KEY `age2_key` (`age2`),
  UNIQUE KEY `class1_key` (`class1`),
  UNIQUE KEY `class2_key` (`class2`),
  KEY `address_key` (`address`)
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50 COMMENT='{"resource_tag":"", "replica_num":3, "region_split_lines":2097152, "namespace":"FENGCHAO"}';

DROP TABLE IF EXISTS `Baikaltest`.`t_student1`;
CREATE TABLE `Baikaltest`.`t_student1` (
  `id` int(10) NOT NULL AUTO_INCREMENT ,
  `name1` varchar(1024) NOT NULL ,
  `name2` varchar(1024) NOT NULL ,
  `age1` int(10) NOT NULL ,
  `age2` int(10) NOT NULL ,
  `class1` int(10) NOT NULL ,
  `class2` int(10) NOT NULL ,
  `address1` varchar(1024) NOT NULL ,
  `address2` varchar(1024) NOT NULL ,
  `height1` int(10) NOT NULL ,
  `height2` int(10) NOT NULL ,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name1_key` (`name1`),
  UNIQUE KEY `name2_key` (`name2`),
  KEY `age1_key` (`age1`),
  KEY `age2_key` (`age2`),
 UNIQUE KEY GLOBAL `class1_key` (`class1`),
 UNIQUE KEY GLOBAL `class2_key` (`class2`),
 KEY GLOBAL `address1_key` (`address1`),
 KEY GLOBAL `address2_key` (`address2`)
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50000 COMMENT='{"resource_tag":"", "replica_num":3, "namespace":"FENGCHAO"}';

DROP TABLE IF EXISTS `Baikaltest`.`t_student2`;
CREATE TABLE `Baikaltest`.`t_student2` (
  `id` int(10) NOT NULL AUTO_INCREMENT ,
  `name1` varchar(1024) NOT NULL ,
  `name2` varchar(1024) NOT NULL ,
  `age1` int(10) NOT NULL ,
  `age2` int(10) NOT NULL ,
  `class1` int(10) NOT NULL ,
  `class2` int(10) NOT NULL ,
  `address1` varchar(1024) NOT NULL ,
  `address2` varchar(1024) NOT NULL ,
  `height1` int(10) NOT NULL ,
  `height2` int(10) NOT NULL ,
  PRIMARY KEY (`id`),
  FULLTEXT KEY `full_idx_name1` (`name1`) COMMENT '{"segment_type":"S_NO_SEGMENT"}',
  FULLTEXT KEY `full_idx_name2` (`name2`) COMMENT '{"segment_type":"S_DEFAULT"}',
  FULLTEXT KEY `full_idx_address1` (`address1`) COMMENT '{"segment_type":"S_UNIGRAMS"}',
  FULLTEXT KEY `full_idx_address2` (`address2`) COMMENT '{"segment_type":"S_WORDSEG_BASIC"}'
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50000 COMMENT='{"resource_tag":"", "replica_num":3, "namespace":"FENGCHAO"}';


DROP TABLE IF EXISTS `Baikaltest`.`t_student3`;
CREATE TABLE `Baikaltest`.`t_student3` (
  `id` int(10) NOT NULL AUTO_INCREMENT ,
  `name1` varchar(1024) NOT NULL ,
  `name2` varchar(1024) NOT NULL ,
  `age1` int(10) NOT NULL ,
  `age2` int(10) NOT NULL ,
  `class1` int(10) NOT NULL ,
  `class2` int(10) NOT NULL ,
  `address1` varchar(1024) NOT NULL ,
  `address2` varchar(1024) NOT NULL ,
  `height1` int(10) NOT NULL ,
  `height2` int(10) NOT NULL ,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name1_key` (`name1`),
  UNIQUE KEY `name2_key` (`name2`),
  KEY `age1_key` (`age1`),
  KEY `age2_key` (`age2`),
 UNIQUE KEY GLOBAL `class1_key` (`class1`),
 UNIQUE KEY GLOBAL `class2_key` (`class2`),
 KEY GLOBAL `address1_key` (`address1`),
 FULLTEXT KEY `full_idx_address2` (`address2`) COMMENT '{"segment_type":"S_BIGRAMS"}'
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50000 COMMENT='{"resource_tag":"", "replica_num":3, "namespace":"FENGCHAO"}';

DROP TABLE IF EXISTS `Baikaltest`.`planinfo`;
CREATE TABLE `Baikaltest`.`planinfo` (
  `planid` int(10) unsigned NOT NULL,
  `userid` int(10) unsigned NOT NULL,
  `planname` char(32) NOT NULL,
  `planstat1` int(10) unsigned NOT NULL,
  `planstat2` int(10) unsigned NOT NULL,
  `showprob` tinyint(3) unsigned NOT NULL,
  `showrate` tinyint(3) unsigned NOT NULL,
  `showfactor` int(10) unsigned NOT NULL DEFAULT '655370000',
  `planmode` tinyint(3) unsigned DEFAULT '0',
  `touch` int(10) unsigned NOT NULL DEFAULT '0',
  `moduid` int(10) unsigned NOT NULL,
  `modtime` datetime NOT NULL,
  `isdel` tinyint(3) unsigned NOT NULL,
  `deviceprefer` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `devicecfgstat` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `ideatype` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `imchannel` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '默认凤巢计划为0,闪投系统为1',
  `mpricefactor` decimal(5,2) NOT NULL DEFAULT '1.00' COMMENT '无线出价比例,范围在0.00-9.99',
  `dynamicideastat` int(10) DEFAULT NULL,
  `remarketingstat` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '默认0普通计划，1再营销计划，2普通且再营销计划',
  `adtype` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `productstatus` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'plan层级的开关状态,0关闭，1启动',
  `bidprefer` tinyint(3) unsigned NOT NULL DEFAULT '1' COMMENT '',
  `pcpricefactor` decimal(5,2) NOT NULL DEFAULT '1.00' COMMENT '',
  `targeturl` varchar(2048) DEFAULT NULL,
  `businesspointid` bigint(20) DEFAULT '0',
  `marketingtargetid` int(32) DEFAULT '0',
  `addfrom` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '',
  `planstat3` int(10) unsigned NOT NULL DEFAULT '0',
  `regiontype` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '0表示使用省市投放；1表示使用区域投放(商圈/门店)',
  `flowSource` int(10) unsigned NOT NULL DEFAULT '0',
  `shoptype` int(10) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`planid`),
  KEY `Index_planinfo_userid` (`userid`),
  KEY `Index_planinfo_planname` (`planname`)
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50 COMMENT='{"resource_tag":"", "replica_num":3, "region_split_lines":100, "namespace":"FENGCHAO"}';

DROP TABLE IF EXISTS `Baikaltest`.`unitinfo`;
CREATE TABLE `Baikaltest`.`unitinfo` (
  `unitid` bigint(20) unsigned NOT NULL ,
  `planid` bigint(20) unsigned NOT NULL ,
  `userid` bigint(20) unsigned NOT NULL ,
  `unitname` varchar(1024) NOT NULL DEFAULT '' ,
  `rstat` int(10) unsigned NOT NULL ,
  `astat` int(10) unsigned NOT NULL ,
  `bidtype` tinyint(4) unsigned NOT NULL DEFAULT '1' ,
  `bid` double NOT NULL ,
  `isdel` tinyint(4) unsigned NOT NULL DEFAULT '0' ,
  `adtype` tinyint(4) unsigned NOT NULL DEFAULT '8' ,
  `materialstyle` int(10) unsigned NOT NULL DEFAULT '0' ,
  `producttype` int(10) unsigned NOT NULL ,
  `ftype` int(10) unsigned NOT NULL DEFAULT '1' ,
  `addtime` DATETIME NOT NULL ,
  `modtime` DATETIME NOT NULL ,
  `moduid` bigint(20) unsigned NOT NULL ,
  `bstype` tinyint(4) unsigned NOT NULL DEFAULT '1' ,
  `atpid` bigint(20) NOT NULL DEFAULT '0' ,
  `haseffectidea` tinyint(4) unsigned NOT NULL DEFAULT '0' ,
  `expmask` bigint(20) unsigned NOT NULL DEFAULT '0' ,
  `querytype` tinyint(4) NOT NULL DEFAULT '1' ,
  `ocpcinfo` varchar(1024) NOT NULL DEFAULT '' ,
  `openflag` int(10) unsigned NOT NULL DEFAULT '1' ,
  PRIMARY KEY (`userid`,`unitid`),
  UNIQUE KEY `index_unitinfo_unitid` (`unitid`),
  KEY `index_unitinfo_planid` (`planid`),
  KEY `unit_modtime` (`modtime`),
  KEY `unitinfo_atpid_idx` (`atpid`,`isdel`),
  FULLTEXT KEY `unitname_key` (`unitname`) COMMENT '{"segment_type":"S_WORDRANK"}'
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50 COMMENT='{"resource_tag":"", "replica_num":3, "region_split_lines":10, "namespace":"FENGCHAO"}';

DROP TABLE IF EXISTS `Baikaltest`.`unitsetting`;
CREATE TABLE `Baikaltest`.`unitsetting` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `unitid` bigint(20) unsigned NOT NULL ,
  `planid` bigint(20) unsigned NOT NULL ,
  `userid` bigint(20) unsigned NOT NULL ,
  `stype` int(10) unsigned NOT NULL ,
  `content` varchar(1024) NOT NULL ,
  `addtime` DATETIME NOT NULL ,
  `modtime` DATETIME NOT NULL ,
  PRIMARY KEY (`userid`,`unitid`,`stype`,`id`),
  KEY `index_unitsetting_unitid` (`unitid`),
  KEY `index_unitsetting_planid` (`planid`),
  KEY `unit_modtime` (`modtime`),
  FULLTEXT KEY `content_key` (`content`) COMMENT '{"segment_type":"S_ES_STANDARD"}'
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50 COMMENT='{"resource_tag":"", "replica_num":3, "region_split_lines":200, "namespace":"FENGCHAO"}';

DROP TABLE IF EXISTS `Baikaltest`.`ideainfo`;
CREATE TABLE `Baikaltest`.`ideainfo` (
  `ideaid` bigint(20) unsigned NOT NULL ,
  `unitid` bigint(20) unsigned NOT NULL ,
  `planid` bigint(20) unsigned NOT NULL ,
  `userid` bigint(20) unsigned NOT NULL ,
  `ideaname` varchar(1024) NOT NULL DEFAULT '' ,
  `rstat` int(10) unsigned NOT NULL ,
  `astat` int(10) unsigned NOT NULL ,
  `content` varchar(1024) NOT NULL ,
  `isdel` tinyint(4) unsigned NOT NULL DEFAULT '0' ,
  `adtype` tinyint(4) unsigned NOT NULL DEFAULT '8' ,
  `ftype` int(10) unsigned NOT NULL DEFAULT '1' ,
  `addtime` DATETIME NOT NULL ,
  `modtime` DATETIME NOT NULL ,
  `moduid` bigint(20) unsigned NOT NULL ,
  `versionid` int(10) unsigned NOT NULL DEFAULT '0' ,
  `bstype` tinyint(4) unsigned NOT NULL DEFAULT '1' ,
  `materialstyle` int(10) unsigned NOT NULL DEFAULT '0' ,
  `riskscore` varchar(1024) NOT NULL DEFAULT 'DFT' ,
  `expmask` bigint(20) unsigned NOT NULL DEFAULT '0' ,
  `showmt` int(10) NOT NULL DEFAULT '0' ,
  `openflag` bigint(20) NOT NULL DEFAULT '0' ,
  `itype` tinyint(4) NOT NULL DEFAULT '0' ,
  `castat` int(10) NOT NULL DEFAULT '0' ,
  PRIMARY KEY (`userid`,`ideaid`),
  KEY `index_unitinfo_userid` (`unitid`),
  UNIQUE KEY `index_ideaid` (`ideaid`),
  KEY `ideainfo_modtime` (`modtime`),
  KEY `index_planid` (`planid`),
  FULLTEXT KEY `ideaname_key` (`ideaname`) COMMENT '{"segment_type":"S_WORDRANK_Q2B_ICASE"}',
  FULLTEXT KEY `riskscore_key` (`riskscore`) COMMENT '{"segment_type":"S_WORDSEG_BASIC"}'
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50 COMMENT='{"resource_tag":"", "replica_num":3, "region_split_lines":200000, "namespace":"FENGCHAO"}';


# this table for ddl
DROP TABLE IF EXISTS `Baikaltest`.`DDLtable`;
CREATE TABLE `Baikaltest`.`DDLtable` (
  `id` int(10) NOT NULL AUTO_INCREMENT ,
  `name1` varchar(1024) NOT NULL ,
  `name2` varchar(1024) NOT NULL ,
  `age1` int(10) NOT NULL ,
  `age2` int(10) NOT NULL ,
  `class1` int(10) NOT NULL ,
  `class2` int(10) NOT NULL ,
  `address1` varchar(1024) NOT NULL ,
  `address2` varchar(1024) NOT NULL ,
  `height1` int(10) NOT NULL ,
  `height2` int(10) NOT NULL ,
  PRIMARY KEY (`id`)
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50000 COMMENT='{"resource_tag":"", "replica_num":3, "namespace":"FENGCHAO"}';


DROP TABLE IF EXISTS `Baikaltest`.`DDLtable2`;
CREATE TABLE `Baikaltest`.`DDLtable2` (
  `id` int(10) NOT NULL AUTO_INCREMENT ,
  `name1` varchar(1024) NOT NULL ,
  `name2` varchar(1024) NOT NULL ,
  `age1` int(10) NOT NULL ,
  `age2` int(10) NOT NULL ,
  `class1` int(10) NOT NULL ,
  `class2` int(10) NOT NULL ,
  `address1` varchar(1024) NOT NULL ,
  `address2` varchar(1024) NOT NULL ,
  `height1` int(10) NOT NULL ,
  `height2` int(10) NOT NULL ,
  PRIMARY KEY (`id`),
  UNIQUE KEY `class1_key` (`class1`),
  KEY `class2_key` (`class2`)
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50000 COMMENT='{"resource_tag":"", "replica_num":3, "namespace":"FENGCHAO"}';

DROP TABLE IF EXISTS `Baikaltest`.`test_1`;
CREATE TABLE `Baikaltest`.`test_1` (
  `id` int(10) NOT NULL AUTO_INCREMENT ,
  `date1` DATETIME NOT NULL ,
  `date2` DATE NOT NULL ,
  `address1` varchar(1024) NOT NULL ,
  `address2` varchar(1024) NOT NULL ,
  `height1` int(10) NOT NULL ,
  `height2` int(10) NOT NULL ,
  PRIMARY KEY (`id`),
  FULLTEXT KEY `address1_key` (`address1`) COMMENT '{"segment_type":"S_ES_STANDARD", "storage_type":"ST_ARROW"}'
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50000 COMMENT='{"resource_tag":"", "replica_num":3, "namespace":"FENGCHAO"}';


DROP TABLE IF EXISTS `Baikaltest`.`student333`;
CREATE TABLE `Baikaltest`.`student333` (
  `id` int(10) NOT NULL ,
  `name1` varchar(1024) NOT NULL ,
  `name2` varchar(1024) NOT NULL ,
  `age1` int(10) NOT NULL ,
  `age2` int(10) NOT NULL ,
  `class1` int(10) NOT NULL ,
  `class2` int(10) NOT NULL ,
  `address` varchar(1024) NOT NULL ,
  `height` int(10) NOT NULL ,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name1_key` (`name1`),
  UNIQUE KEY `name2_key` (`name2`),
  KEY `age1_key` (`age1`),
  KEY `age2_key` (`age2`),
  UNIQUE KEY `class1_key` (`class1`),
  UNIQUE KEY `class2_key` (`class2`),
  KEY `address_key` (`address`)
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50 COMMENT='{"resource_tag":"", "replica_num":3, "region_split_lines":2097152, "namespace":"FENGCHAO"}';


DROP TABLE IF EXISTS `Baikaltest`.`insert_select`;
CREATE TABLE `Baikaltest`.`insert_select` (
  `id` int(10) NOT NULL AUTO_INCREMENT ,
    `name1` varchar(1024) NOT NULL ,
    `name2` varchar(1024) NOT NULL ,
    `age1` int(10) NOT NULL ,
    `age2` int(10) NOT NULL ,
    `class1` int(10) NOT NULL ,
    `class2` int(10) NOT NULL ,
    `address1` varchar(1024) NOT NULL ,
    `address2` varchar(1024) NOT NULL ,
    `height1` int(10) NOT NULL ,
    `height2` int(10) NOT NULL ,
    PRIMARY KEY (`id`),
    UNIQUE KEY `name1_key` (`name1`),
    UNIQUE KEY `name2_key` (`name2`),
    KEY `age1_key` (`age1`),
    KEY `age2_key` (`age2`),
   UNIQUE KEY GLOBAL `class1_key` (`class1`),
   UNIQUE KEY GLOBAL `class2_key` (`class2`)
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50 COMMENT='{"resource_tag":"", "replica_num":3, "region_split_lines":2097152, "namespace":"FENGCHAO"}';


DROP TABLE IF EXISTS `Baikaltest`.`insert`;
CREATE TABLE `Baikaltest`.`insert` (
  `id` int(10) NOT NULL AUTO_INCREMENT ,
  `name1` varchar(1024) NOT NULL ,
  `name2` varchar(1024) NOT NULL ,
  `age1` int(10) NOT NULL ,
  `age2` int(10) NOT NULL ,
  `class1` int(10) NOT NULL ,
  `class2` int(10) NOT NULL ,
  `address1` varchar(1024) NOT NULL ,
  `address2` varchar(1024) NOT NULL ,
  `height1` int(10) NOT NULL ,
  `height2` int(10) NOT NULL ,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name1_key` (`name1`),
  UNIQUE KEY `name2_key` (`name2`),
  KEY `age1_key` (`age1`),
  KEY `age2_key` (`age2`),
 UNIQUE KEY GLOBAL `class1_key` (`class1`),
 UNIQUE KEY GLOBAL `class2_key` (`class2`),
 KEY GLOBAL `address1_key` (`address1`),
 KEY GLOBAL `address2_key` (`address2`)
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50000 COMMENT='{"resource_tag":"", "replica_num":3, "namespace":"FENGCHAO"}';



DROP TABLE IF EXISTS `Baikaltest`.`update`;
CREATE TABLE `Baikaltest`.`update` (
  `id` int(10) NOT NULL AUTO_INCREMENT ,
  `name1` varchar(1024) NOT NULL ,
  `name2` varchar(1024) NOT NULL ,
  `age1` int(10) NOT NULL ,
  `age2` int(10) NOT NULL ,
  `class1` int(10) NOT NULL ,
  `class2` int(10) NOT NULL ,
  `address1` varchar(1024) NOT NULL ,
  `address2` varchar(1024) NOT NULL ,
  `height1` int(10) NOT NULL ,
  `height2` int(10) NOT NULL ,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name1_key` (`name1`),
  UNIQUE KEY `name2_key` (`name2`),
  KEY `age1_key` (`age1`),
  KEY `age2_key` (`age2`),
 UNIQUE KEY GLOBAL `class1_key` (`class1`),
 UNIQUE KEY GLOBAL `class2_key` (`class2`),
 KEY GLOBAL `address1_key` (`address1`),
 KEY GLOBAL `address2_key` (`address2`)
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50000 COMMENT='{"resource_tag":"", "replica_num":3, "namespace":"FENGCHAO"}';


DROP TABLE IF EXISTS `Baikaltest`.`delete`;
CREATE TABLE `Baikaltest`.`delete` (
  `id` int(10) NOT NULL AUTO_INCREMENT ,
  `name1` varchar(1024) NOT NULL ,
  `name2` varchar(1024) NOT NULL ,
  `age1` int(10) NOT NULL ,
  `age2` int(10) NOT NULL ,
  `class1` int(10) NOT NULL ,
  `class2` int(10) NOT NULL ,
  `address1` varchar(1024) NOT NULL ,
  `address2` varchar(1024) NOT NULL ,
  `height1` int(10) NOT NULL ,
  `height2` int(10) NOT NULL ,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name1_key` (`name1`),
  UNIQUE KEY `name2_key` (`name2`),
  KEY `age1_key` (`age1`),
  KEY `age2_key` (`age2`),
 UNIQUE KEY GLOBAL `class1_key` (`class1`),
 UNIQUE KEY GLOBAL `class2_key` (`class2`),
 KEY GLOBAL `address1_key` (`address1`),
 KEY GLOBAL `address2_key` (`address2`)
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50000 COMMENT='{"resource_tag":"", "replica_num":3, "namespace":"FENGCHAO"}';


DROP TABLE IF EXISTS `Baikaltest`.`select`;
CREATE TABLE `Baikaltest`.`select` (
  `id` int(10) NOT NULL AUTO_INCREMENT ,
  `name1` varchar(1024) NOT NULL ,
  `name2` varchar(1024) NOT NULL ,
  `age1` int(10) NOT NULL ,
  `age2` int(10) NOT NULL ,
  `class1` int(10) NOT NULL ,
  `class2` int(10) NOT NULL ,
  `address1` varchar(1024) NOT NULL ,
  `address2` varchar(1024) NOT NULL ,
  `height1` int(10) NOT NULL ,
  `height2` int(10) NOT NULL ,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name1_key` (`name1`),
  UNIQUE KEY `name2_key` (`name2`),
  KEY `age1_key` (`age1`),
  KEY `age2_key` (`age2`),
 UNIQUE KEY GLOBAL `class1_key` (`class1`),
 UNIQUE KEY GLOBAL `class2_key` (`class2`),
 KEY GLOBAL `address1_key` (`address1`),
 KEY GLOBAL `address2_key` (`address2`)
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50000 COMMENT='{"resource_tag":"", "replica_num":3, "namespace":"FENGCHAO"}';


DROP TABLE IF EXISTS `Baikaltest`.`subselect`;
CREATE TABLE `Baikaltest`.`subselect` (
  `id` int(10) NOT NULL AUTO_INCREMENT ,
  `name1` varchar(1024) NOT NULL ,
  `name2` varchar(1024) NOT NULL ,
  `age1` int(10) NOT NULL ,
  `age2` int(10) NOT NULL ,
  `class1` int(10) NOT NULL ,
  `class2` int(10) NOT NULL ,
  `address1` varchar(1024) NOT NULL ,
  `address2` varchar(1024) NOT NULL ,
  `height1` int(10) NOT NULL ,
  `height2` int(10) NOT NULL ,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name1_key` (`name1`),
  UNIQUE KEY `name2_key` (`name2`),
  KEY `age1_key` (`age1`),
  KEY `age2_key` (`age2`),
 UNIQUE KEY GLOBAL `class1_key` (`class1`),
 UNIQUE KEY GLOBAL `class2_key` (`class2`),
 KEY GLOBAL `address1_key` (`address1`),
 KEY GLOBAL `address2_key` (`address2`)
) ENGINE=Rocksdb_cstore DEFAULT CHARSET=gbk AVG_ROW_LENGTH=50000 COMMENT='{"resource_tag":"", "replica_num":3, "namespace":"FENGCHAO"}';
