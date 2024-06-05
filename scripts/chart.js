const chartEl = document.getElementById("chart");
const chart = echarts.init(chartEl);

const chartOption = {
  grid: {
    left: "1%",
    right: "4%",
    bottom: "3%",
    containLabel: true,
  },
  xAxis: {
    name: "Size",
    type: "category",
    boundaryGap: false,
    data: [],
    axisLabel: {
      formatter: function (value) {
        return value + "B";
      },
    },
  },
  yAxis: {
    name: "Pass@1(Class-wise)",
    type: "value",
    show: true,
    nameTextStyle: {
      align: "left",
    },
    splitLine: {
      show: true,
      lineStyle: {
        type: "dashed",
      },
    },
  },
  tooltip: {
    trigger: "item",
    axisPointer: {
      type: "cross",
    },
  },
  series: [],
};

async function displayData(data, metric) {
  const models = await fetch("models.json").then((resp) => resp.json());
  const flattened = flatten(data);

  const modelSizes = models.map((m) => Math.round(m.size)).filter(Boolean);
  modelSizes.sort((a, b) => a - b);

  chartOption.xAxis.data = modelSizes;
  chartOption.series = [];

  const series = [...new Set(flattened.map((row) => row[metric]))];
  chartOption.legend = series.map((name) => ({ name }));

  for (const serieName of series) {
    const serie = {
      name: serieName,
      type: "scatter",
      data: [],
      markLine: {
        symbol: "none",
        emphasis: {
          label: {
            position: "middle",
            formatter: function (params) {
              return params.data.name;
            },
          },
        },
        data: [],
      },
    };

    const data = flattened.filter((row) => row[metric] === serieName);
    for (const row of data) {
      const modelSize = models.find((m) => m.model === row.model)?.size;
      if (modelSize !== undefined) {
        serie.data.push({
          name: `${row["model"]}(${serieName})`,
          value: [`${Math.round(modelSize)}`, row["pass_class_wise"]],
        });
      } else {
        serie.markLine.data.push({
          name: `${row["model"]}(${serieName})`,
          yAxis: row["pass_class_wise"],
        });
      }
    }
    chartOption.series.push(serie);
  }

  chart.setOption(chartOption);
}

btnMethod.addEventListener("click", () => {
  fetch("data/data_method.json")
    .then((resp) => resp.json())
    .then((data) => {
      displayData(data, "method");
    });
});

btnContext.addEventListener("click", () => {
  fetch("data/data_context.json")
    .then((resp) => resp.json())
    .then((data) => {
      displayData(data, "context");
    });
});

btnIncrOrder.addEventListener("click", () => {
  fetch("data/data_incr-order.json")
    .then((resp) => resp.json())
    .then((data) => {
      displayData(data, "method");
    });
});

fetch("data/data_method.json")
  .then((resp) => resp.json())
  .then((data) => {
    displayData(data, "method");
  });

window.addEventListener("resize", () => {
  chart.resize();
});
