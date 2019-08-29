import React from 'react';
import ReactTable, {CellInfo} from 'react-table';
import {fetchJsonFromServer} from './service';
import {ErrorContainer} from './Error';
import {Progress} from 'reactstrap';

interface Props {
    match: any,
    err: ErrorContainer
}

interface ExecutorSummary {
    id: string,
    type: string,
    version: string,
    resources: string,
}

interface State {
    data: ExecutorSummary[],
    loading: boolean,
}

class Executors extends React.Component<Props, State> {

    constructor(props : Props) {
        super(props);
        this.state = {data: [], loading: true}
    }

    componentDidMount() {
        if (this.props.err.isOk) {
            fetchJsonFromServer("executors", null, "GET").then((data) => {
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

    get name() : string {
        return this.props.match.params.name;
    }

    _cellStatus = (cellInfo : CellInfo) => {
        let v = cellInfo.value;
        return (<span className={"executor-status-" + v}>{v}</span>);
    };

    _cellStats = (cellInfo: CellInfo) => {
        let v = cellInfo.value;
        if (v && v.n_tasks > 0) {
            return (<Progress value={v.n_completed} max={v.n_tasks}>{v.n_completed}/{v.n_tasks}</Progress>);
        } else {
            return "";
        }
    };

    render() {
        const columns = [
            {
                "id": "status",
                "Header": "Status",
                "accessor": "status",
                "Cell": this._cellStatus,
                sortMethod: (a: string, b: string) => {
                    // Sort priority decreases from left to right
                    let values = ["running", "stopped", "lost"];
                    return values.indexOf(a) < values.indexOf(b) ? 1 : -1;
                },
                maxWidth: 200
            },
            {
                "Header": "Id",
                "accessor": "id",
                maxWidth: 100
            },
            {
                "Header": "Type",
                "accessor": "type"
            },
            {
                "Header": "Resources",
                "accessor": "resources"
            },
            {
                "Header": "Tasks",
                "accessor": "stats",
                "Cell": this._cellStats
            },

        ];
        return (
            <div>
            <h1>Executors</h1>
            <ReactTable
                data={this.state.data}
                loading={this.state.loading}
                columns={columns}
                defaultSorted={[{
                    id: "status",
                    desc: true
                }, {
                    id: "id",
                    desc: true
                }]} />
            </div>
        );
    }
}

export default Executors;
