import React from 'react';
import { Select, Popover } from 'antd';
import { datasetTypes } from './actions';

const MIN_WIDTH = 120;
const MAX_COLUMNS = 10;
const SCREEN_WIDTH = document.body.clientWidth - 100;

const typeArray = [
    'integer',
    'bigint',
    'float',
    'double',
    'decimal',
    'boolean',
    'string',
    'varchar',
    'date',
    'timestamp',
    'time'
];

export function getTableWidth(number) {
    let width = '100%';
    if(number <= MAX_COLUMNS || Math.floor(SCREEN_WIDTH/number) >= MIN_WIDTH) {
        width = '100%';
    }else {
        width = MIN_WIDTH * number;
    }
    return width;
}

export function getColumnWidth(number) {
    let width = MIN_WIDTH;
    if(number <= MAX_COLUMNS || Math.floor(SCREEN_WIDTH/number) >= MIN_WIDTH) {
        width = Math.floor(100/number) + '%';
    }else {
        width = MIN_WIDTH;
    }
    return width;
}

export function getHDFSInputColumnWidth(width, colNum) {
    let colWidth = '';
    if(width === '100%') {
        colWidth = 100 / colNum + '%';
    }else {
        colWidth = width / colNum;
    }
    return colWidth;
}

export function getTbTitle(data, width) {
    let tbTitle = [];
    data.columns.map((column) => {
        let objTitle = {
            title: column,
            dataIndex: column,
            key: column,
            width: width,
            render: (text, record) => {
                return (<span
                    className="preview-tb-cell"
                    style={{width: width - 8}}
                    title={text}
                >
                    {text}
                </span>)
            }
        };
        tbTitle.push(objTitle);
    });
    return tbTitle;
}

export function getTbTitleInceptor(commonTitle, width) {
    let tbTitle = commonTitle;
    tbTitle.map(column => {
        column.render = (text) => {
            return (
                <span
                    style={{width: width-5}}
                    className="column-type"
                >
                    {text}
                </span>
            )
        }
    });
    return tbTitle;
}

export function getTbTitleHDFS(commonTitle, _this, data, isFirst) {
    const types = makeDataTypes(data, isFirst);
    const Option = Select.Option;
    const options = typeArray.map(type => {
        return <Option key={type}>{type}</Option>
    });
    let tbTitle = commonTitle;
    tbTitle.map((column, index) => {
        column.render = () => {
            column.type = types[index];
            return (
                <Select
                    defaultValue={types[index]?types[index]:'string'}
                    style={{ width: '100%'}}
                    onSelect={ (value)=>_this.onTypeSelect(value, column) }
                    placeholder='Selet Data Type'
                    getPopupContainer={() => document.getElementById('data-preview-id')}
                >
                    {options}
                </Select>
            )
        }
    });
    return tbTitle;
}

export function setHDFSColumnType(selColumn, tbTitle, typeStr, _this) {
    let typeArrayAccuracy = setTypeArrayAccuracy(typeStr);
    const Option = Select.Option;
    const options = typeArrayAccuracy.map(type => {
        return <Option key={type}>{type}</Option>
    });
    tbTitle.map(column => {
        if(column.key === selColumn.key) {
            column.type = typeStr;
            column.render = () => {
                return (
                    <Select
                        style={{ width: '100%' }}
                        value={typeStr}
                        onSelect={ (value)=>_this.onTypeSelect(value, column) }
                        placeholder='select the type'>
                        {options}
                    </Select>
                )
            }
        }
    });
    return tbTitle;
}

function setTypeArrayAccuracy(typeStr) {
    let typeArrayAccuracy = typeArray.concat();
    if(!(typeStr.indexOf('varchar') > -1 || typeStr.indexOf('decimal') > -1)) {
        return typeArrayAccuracy;
    }
    let k = 0;
    typeArrayAccuracy.map((item, index) => {
        if(typeStr.indexOf(item) === 0) {
            k = index;
        }
    });
    typeArrayAccuracy.splice(k, 1, typeStr);
    return typeArrayAccuracy;
}

export function getTbContent(data) {
    let tbContent = [];
    data.records.map((record, index) => {
        let contentObj = {};
        contentObj.key = index + 1;
        for(var attr in record) {
            contentObj[attr] = record[attr];
        }
        tbContent.push(contentObj);
    });
    return tbContent;
}

export function getTbType(data) {
    let types = data.types;
    let objType = {};
    let tbType = [];
    data.columns.map((column, index) => {
        objType[column] = types[index];
    });
    objType.key = 'type';
    tbType.push(objType);
    return tbType;
}

export function constructInceptorDataset(dataset) {
    let inceptorDataset = {};
    inceptorDataset.dataset_type = datasetTypes.database;
    inceptorDataset.dataset_name = dataset.dataset_name;
    inceptorDataset.table_name = dataset.table_name;
    inceptorDataset.schema = dataset.schema;
    inceptorDataset.database_id = dataset.database_id;
    inceptorDataset.db_name = dataset.db_name;
    inceptorDataset.sql = dataset.sql || '';
    inceptorDataset.description = dataset.description || '';

    return inceptorDataset;
}

export function constructHDFSDataset(state) {
    const { dsHDFS, columnNames } = state;
    const hdfsDataset = {
        dataset_name: dsHDFS.dataset_name,
        database_id: dsHDFS.inceptorConnectId,
        description: dsHDFS.description,
        hdfs_path: dsHDFS.hdfsPath,
        separator: dsHDFS.separator,
        columns: constructHDFSColumns(columnNames),
        file_type: dsHDFS.file_type,
        quote: dsHDFS.quote,
        next_as_header: dsHDFS.next_as_header,
        charset: dsHDFS.charset,
        dataset_type: datasetTypes.hdfs
    };
    return hdfsDataset;
}

function constructHDFSColumns(title) {
    let names = [];
    let types = [];
    let columns = {
        names: names,
        types: types
    };
    if(title) {
        title.map(t => {
            names.push(t.props.value);
            types.push(t.props.className || 'string');
        });
    }
    return columns;
}

export function constructFileBrowserData(data) {
    let fbData = [];
    data.files.map(file => {
        if(!(file.type==='dir'&&(file.name==='..'||file.name==='.'))) {
            let node = {
                label: file.name,
                value: file.path,
                key: file.name,
                category: file.type,
                isLeaf: file.type === 'file' ? true : false
            };
            fbData.push(node);
        }
    });
    return fbData;
}

export function appendTreeChildren(path, children, fbData) {
    let treeNode = findTreeNode(fbData, path);
    if(treeNode) {
        treeNode.children = constructFileBrowserData(children);
    }
    return fbData;
}

export function findTreeNode(treeNodes, path) {
    let node = undefined;
    if (!treeNodes || !treeNodes.length) {
        return node;
    }
    let stack = [], item;
    for (let i = 0; i < treeNodes.length; i++) {
        stack.push(treeNodes[i]);
    }
    while (stack.length) {
        item = stack.shift();
        if(item.value === path) {
            node = item;
            break;
        }
        if (item.children && item.children.length) {

            stack = stack.concat(item.children);
        }
    }

    return node;
}

export function getDatasetTitle(opeType, datasetType, intl) {
    let opeName = '';
    if(opeType === 'add') {
        opeName = intl.get('DATASET.ADD');
    }else if(opeType === 'edit') {
        opeName = intl.get('DATASET.EDIT');
    }
    return opeName + ' ' + datasetType + ' ' + intl.get('DATASET.DATASET');
}

export function getDatasetTab2Name(datasetType, intl) {
    let tab2Name = intl.get('DATASET.CONFIG');
    if(datasetType === datasetTypes.database) {
        tab2Name = intl.get('DATASET.DATA_PREVIEW');
    }
    return tab2Name;
}

export function extractDatasetType(url) {
    const pathArray = url.split('/');
    let type = pathArray[pathArray.length - 2];
    return type;
}

export function extractOpeType(url) {
    const pathArray = url.split('/');
    return pathArray[1];
}

export function extractOpeName(url) {
    const pathArray = url.split('/');
    return pathArray[2];
}

export function getDatasetId(opeType, pathname) {
    let id = '';
    const pathArray = pathname.split('/');
    if(opeType === 'edit') {
        id = pathArray[pathArray.length - 1];
    }
    return id;
}

export function judgeEnableClick(opeType, datasetId) {
    if(opeType === 'edit') {
        return true;
    }
    if(datasetId && datasetId !== '') {
        return true;
    }
    return false;
}

export function judgeEnableClickHDFSPreview(opeType, datasetType, datasetId, HDFSConfigured) {
    if(opeType === 'edit') {
        return true;
    }
    if(datasetId && datasetId !== '') {
        return true;
    }
    if((datasetType === datasetTypes.hdfs || datasetType === datasetTypes.uploadFile) && HDFSConfigured) {
        return true;
    }

    return false;
}

export function initDatasetData(type, newData, oldData) {
    let data = {};
    switch(type) {
        case datasetTypes.database:
            data = initInceptorData(newData, oldData);
            break;
        case datasetTypes.hdfs:
            data = initHDFSData(newData, oldData);
            break;
        default:
            break;
    }
    return data;
}

function initInceptorData(newData, oldData) {
    let inceptorData = {
        dataset_type: datasetTypes.database,
        dataset_name: newData.dataset_name,
        table_name: newData.table_name,
        schema: newData.schema,
        database_id: newData.database_id,
        sql: newData.sql || '',//for remove warning
        description: newData.description,
        databases: oldData.databases,
        treeData: oldData.treeData,
        db_name: oldData.db_name
    };
    return inceptorData;
}

function initHDFSData(newData, oldData) {
    let hdfsData = {
        dataset_type: datasetTypes.hdfs,
        dataset_name: newData.dataset_name,
        description: newData.description,
        hdfsPath: newData.hdfs_path,
        charset: newData.charset,
        file_type: newData.file_type,
        next_as_header: newData.next_as_header,
        quote: newData.quote,
        separator: newData.separator,
        hdfsConnections: oldData.hdfsConnections,
        inceptorConnections: oldData.inceptorConnections,
        hdfsConnectId: oldData.hdfsConnectId,
        inceptorConnectId: oldData.inceptorConnectId,
        hdfsConnectName: oldData.hdfsConnectName,
        inceptorConnectName: oldData.inceptorConnectName,
        fileBrowserData: oldData.fileBrowserData
    };
    return hdfsData;
}

export function initHDFSPreviewData(data, opeType) {
    let dataset = data;
    if(opeType === "add") {
        dataset.file_type = "csv";
        dataset.separator = ",";
        dataset.quote = "\\";
        dataset.next_as_header = false;
        dataset.charset = "utf-8";
    }
    return dataset;
}

export function isActive(type, location) {
    return location.pathname.indexOf(`/${type}/${datasetTypes.database}`) > -1 ||
        location.pathname.indexOf(`/${type}/${datasetTypes.hdfs}`) > -1 ||
        location.pathname.indexOf(`/${type}/${datasetTypes.uploadFile}`) > -1;
}

export function constructHDFSPreviewUrl(dataset, id) {
    let url = window.location.origin + '/hdfstable/preview/';
    url += '?path=' + dataset.hdfsPath + '&separator=' + dataset.separator +
           '&quote=' + dataset.quote + '&next_as_header=' + dataset.next_as_header +
           '&charset=' + dataset.charset + '&dataset_id=' + id;
    return url;
}

export function constructInceptorPreviewUrl(baseUrl, datasetId, dsInceptor) {
    return baseUrl + 'preview_data?dataset_id=' + datasetId + '&database_id=' +
           dsInceptor.database_id + '&full_tb_name=' + dsInceptor.schema + '.' +
           dsInceptor.table_name;

}

export function makeDataTypes(data, isFirst) {
    let dataTypes = [];
    if(isFirst) {
        dataTypes = data;
    }else {
        data.map(item => {
            dataTypes.push(item.type);
        });
    }
    return dataTypes;
}

export function getMetricTypeOptions() {
    const types = ['count', 'count_distinct', 'sum', 'avg', 'max', 'min'];
    const options = types.map(type => {
        return <Select.Option key={type}>{type}</Select.Option>
    });
    return options;
}

