import React from 'react';
import {Container} from 'unstated';
import {Alert} from "reactstrap";


type ErrorState = {
  message: string | null,
};

export class ErrorContainer extends Container<ErrorState> {
  state = {
    message: null
  };

  get hasError() {
      return this.state.message !== null;
  }

  get isOk() {
      return this.state.message === null;
  }

  setError(message: string) {
      this.setState({message: message});
  }

  setFetchError() {
      this.setError("Fetching of data failed");
  }
}


export const ErrorDisplay: React.FC<{err: ErrorContainer}> = (props) => {
    if (props.err.isOk) {
        return null;
    } else {
        return (<Alert color="danger">{props.err.state.message}</Alert>);
    }
}