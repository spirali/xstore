from typing import Iterable

from .ref import Ref


class Task:
    def __init__(self, ref: Ref, inputs: Iterable["Task"]):
        self.ref = ref
        self.inputs = inputs
