#!/usr/bin/env python3

import aws_cdk as cdk

from cdktest.cdktest_stack import CdktestStack
# from cdktest.trigger_stack import TriggerStack


app = cdk.App()
CdktestStack(app, "CdktestStack")
# TriggerStack(app, "TriggerStack")

app.synth()
