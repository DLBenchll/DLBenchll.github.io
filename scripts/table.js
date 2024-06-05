const tableClass = document.getElementById("class-wise");
const tableTest = document.getElementById("test-wise");

function flatten(data) {
  const results = [];
  for (const [key, value] of Object.entries(data)) {
    for (const row of value) {
      const {
        Context: context,
        Method: method,
        Model: model,
        Pass_at_1: score,
      } = row;

      let result = results.find(
        (r) => r.context === context && r.method === method && r.model === model
      );
      if (!result) {
        result = { context, method, model };
        results.push(result);
      }

      result[key] = score;
    }
  }
  return results;
}

async function displayData(data, metric, metricName) {
  const models = await fetch("models.json").then((resp) => resp.json());

  const flattened = flatten(data);
  [...document.querySelectorAll("[data-metric]")].forEach(
    (el) => (el.innerHTML = metricName)
  );

  function display(el, cols) {
    flattened.sort(
      (a, b) =>
        cols.reduce((acc, key) => acc + b[key], 0) -
        cols.reduce((acc, key) => acc + a[key], 0)
    );

    const tbody = el.querySelector("tbody");
    tbody.innerHTML = "";
    for (const [index, row] of flattened.entries()) {
      const tr = document.createElement("tr");
      const tdIndex = document.createElement("td");
      tdIndex.textContent = index + 1;
      tr.appendChild(tdIndex);

      for (const col of ["model", metric, ...cols]) {
        const td = document.createElement("td");
        if (col === "model") {
          const anchor = document.createElement("a");
          anchor.href = models.find((m) => m.model === row.model).link;
          anchor.innerHTML = row.model;
          td.appendChild(anchor);
        } else if (typeof row[col] === "number") {
          td.innerHTML = (row[col] * 100).toFixed(1);
        } else {
          td.innerHTML = row[col];
        }
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
  }

  display(tableClass, [
    "completion",
    "compilation_class_wise",
    "pass_class_wise",
  ]);
  display(tableTest, ["compilation_test_wise", "pass_test_wise"]);
}

const btnMethod = document.getElementById("method");
const btnContext = document.getElementById("context");
const btnIncrOrder = document.getElementById("incr-order");

btnMethod.addEventListener("click", () => {
  fetch("data/data_method.json")
    .then((resp) => resp.json())
    .then((data) => {
      displayData(data, "method", "Method");
    });
});

btnContext.addEventListener("click", () => {
  fetch("data/data_context.json")
    .then((resp) => resp.json())
    .then((data) => {
      displayData(data, "context", "Context");
    });
});

btnIncrOrder.addEventListener("click", () => {
  fetch("data/data_incr-order.json")
    .then((resp) => resp.json())
    .then((data) => {
      displayData(data, "method", "Method");
    });
});

fetch("data/data_method.json")
  .then((resp) => resp.json())
  .then((data) => {
    displayData(data, "method", "Method");
  });
