const btnMethod = document.getElementById("method");
const btnContext = document.getElementById("context");
const btnIncrOrder = document.getElementById("incr-order");

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

      result[key] = (score * 100).toFixed(1);
    }
  }
  return results;
}
