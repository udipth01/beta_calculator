async function calculate() {
  const fileInput = document.querySelector('input[type="file"]');
  const dateInput = document.getElementById("valuation_date");

  if (!fileInput.files.length) {
    alert("Please upload at least one file");
    return;
  }

  const formData = new FormData();

  // IMPORTANT: key must be `files`
  for (const file of fileInput.files) {
    formData.append("files", file);
  }

  let url = "/portfolio/beta";
  if (dateInput.value) {
    url += `?valuation_date=${dateInput.value}`;
  }

  try {
    const response = await fetch(url, {
      method: "POST",
      body: formData
    });

    const data = await response.json();

    if (!response.ok) {
      alert(data.detail || "Error calculating beta");
      return;
    }

    renderResult(data);
  } catch (err) {
    console.error(err);
    alert("Server error");
  }
}

function renderResult(data) {
  const summary = document.getElementById("summary");
  const table = document.getElementById("result-table");

  summary.innerHTML = `
    Portfolio Beta: <b>${data.portfolio_beta}</b><br>
    Total Value: <b>₹${data.total_value}</b>
  `;

  table.innerHTML = "";

  if (!data.details || !data.details.length) {
    table.innerHTML = "<tr><td>No data</td></tr>";
    return;
  }

  const headers = Object.keys(data.details[0]);
  const thead = document.createElement("tr");

  headers.forEach(h => {
    const th = document.createElement("th");
    th.textContent = h;
    thead.appendChild(th);
  });

  table.appendChild(thead);

  data.details.forEach(row => {
    const tr = document.createElement("tr");
    headers.forEach(h => {
      const td = document.createElement("td");
      td.textContent = row[h] ?? "";
      tr.appendChild(td);
    });
    table.appendChild(tr);
  });
}
