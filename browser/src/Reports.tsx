import React from 'react';
import ReactTable, {CellInfo} from 'react-table';
import {fetchJsonFromServer} from './service';
import {ErrorContainer} from './Error';

interface Props {
    err: ErrorContainer
}

interface Report {
    type: string,
    message: string,
    executor: number,
    config: any,
    collection: string | null,
}

interface State {
    data: Report[],
    loading: boolean,
}

class Reports extends React.Component<Props, State> {

    constructor(props : Props) {
        super(props);
        this.state = {data: [], loading: true}
    }

    componentDidMount() {
        if (this.props.err.isOk) {
            fetchJsonFromServer("reports", null, "GET").then((data) => {
                this.setState({
                    data: data,
                    loading: false
                });
            }).catch((error) => {
               console.log(error);
               this.props.err.setFetchError();
            });
        }
    }

    /*_collectionCell = (cellInfo: CellInfo) => {
        const row: CollectionSummary = cellInfo.row;
        return (<Link to={"/collection/" + row.name}>{row.name}</Link>);
    }*/

    _cellType = (cellInfo : CellInfo) => {
        let v = cellInfo.value;
        return (<span className={"report-type-" + v}>{v}</span>);
    };

    _cellConfig = (cellInfo : CellInfo) => {
        let config = cellInfo.value;
        if (config) {
            return JSON.stringify(config);
        } else {
            return ""
        }
    };

    render() {
        const columns = [{
            Header: "Timestamp",
            accessor: "timestamp",
            maxWidth: 200,
            //Cell: this._collectionCell,
        },
        {
            Header: "Type",
            accessor: "type",
            maxWidth: 100,
            Cell: this._cellType
        },
        {
            id: "Message",
            Header: "Message",
            accessor: "message"
        },
        {
            id: "Executor",
            Header: "Executor",
            accessor: "executor",
            maxWidth: 100
        },
        {
            id: "Collection",
            Header: "Collection",
            accessor: "collection",
            maxWidth: 200
        },
        {
            id: "Config",
            Header: "Config",
            accessor: "config",
            Cell: this._cellConfig,
        }
        ];

        return (
            <div>
            <h1>Collections</h1>
            <ReactTable data={this.state.data} loading={this.state.loading} columns={columns}/>
            </div>
        );
    }
}

export default Reports;
