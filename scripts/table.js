const tableClass = document.getElementById("class-wise");
const tableTest = document.getElementById("test-wise");

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
