apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: output-parameter-
spec:
  entrypoint: main
  templates:
  - name: main
    steps:
    - - name: step1
        template: generate-csv
    - - name: step2
        template: update-db
        arguments:
          artifacts:
          - name: abc
            from: "{{steps.step1.outputs.artifacts.csv}}"

  - name: generate-csv
    container:
      image: qiandahao/cn-kline-day
      command: ["python"]
      args: ["kline_day_us.py"]
    outputs:
      artifacts:
      - name: csv
        path: /csv

  - name: update-db
    inputs:
      artifacts:
      - name: abc
        path: /usr/local
    container:
      image: clickhouse/clickhouse-server:latest
      command: ["/bin/sh","-c"]
      args:
      - |
        cd /usr/local/csv
        sh ./import.sh