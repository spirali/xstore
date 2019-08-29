import React from 'react';
import ReactTable, {CellInfo} from 'react-table';
import {fetchJsonFromServer} from './service';
import {formatSize} from './utils';
import {Link} from 'react-router-dom';
import {ErrorContainer} from './Error';

interface Props {
    err: ErrorContainer
}

interface CollectionSummary {
    name: string,
    count: number,
    size: number,
}

interface State {
    data: CollectionSummary[],
    loading: boolean,
}

class Collections extends React.Component<Props, State> {

    constructor(props : Props) {
        super(props);
        this.state = {data: [], loading: true}
    }

    componentDidMount() {
        if (this.props.err.isOk) {
            fetchJsonFromServer("collections", null, "GET").then((data) => {
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

    _formatSize = (collection : CollectionSummary) => formatSize(collection.size)
    _collectionCell = (cellInfo: CellInfo) => {
        const row: CollectionSummary = cellInfo.row;
        return (<Link to={"/collection/" + row.name}>{row.name}</Link>);
    }

    render() {
        const columns = [{
            Header: "Collection name",
            accessor: "name",
            Cell: this._collectionCell,
        },
        {
            Header: "Entries",
            accessor: "count",
            maxWidth: 100,
        },
        {
            id: "Size",
            Header: "Size",
            accessor: this._formatSize,
            maxWidth: 200
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

export default Collections;
