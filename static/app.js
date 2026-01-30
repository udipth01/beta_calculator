async function calculate() {
  const fileInput = document.getElementById("file");
  const dateInput = document.getElementById("valuation_date");

  if (!fileInput.files.length) {
    alert("Please upload a file");
    return;
  }

  const formData = new FormData();
  formData.append("file", fileInput.files[0]);

  let url = "/portfolio/beta";
  if (dateInput.value) {
    url += `?valuation_date=${dateInput.value}`;
  }

  const response = await fetch(url, {
    method: "POST",
    body: formData
  });

  const data = await response.json();

  if (!response.ok) {
    alert(data.detail || "Error");
    return;
  }

  document.getElementById("summary").innerText =
    `Portfolio Beta: ${data.portfolio_beta} | Total Value: ₹${data.total_value}`;

  const table = document.getElementById("result-table");
  table.innerHTML = "";

  if (!data.details.length) return;

  const headers = Object.keys(data.details[0]);
  table.innerHTML += "<tr>" + headers.map(h => `<th>${h}</th>`).join("") + "</tr>";

  data.details.forEach(row => {
    table.innerHTML += "<tr>" +
      headers.map(h => `<td>${row[h] ?? ""}</td>`).join("") +
      "</tr>";
  });
}
