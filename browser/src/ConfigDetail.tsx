import React from "react";

export function ConfigDetail(config: string, items: [{ header: string, value: string }]): JSX.Element {
    let rows = [];
    for (let item of items) {
        rows.push(
            <div>
                <h5 className="detail-header">{item.header}</h5>
                <pre className="detail-value">{item.value}</pre>
            </div>
        );
    }

    return (
        <div>
            {config &&
                <div>
                    <h5 className="detail-header">Config</h5>
                    <pre className="detail-value">{JSON.stringify(config, null, 2)}</pre>
                </div>
            }
            {rows}
        </div>
    );
}
