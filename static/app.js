async function calculate() {
  const fileInput = document.querySelector('input[type="file"]');
  const dateInput = document.getElementById("valuation_date");
  const status = document.getElementById("status");
  const spinner = document.getElementById("spinner");
  const button = document.getElementById("calc-btn");

  if (!fileInput.files.length) {
    alert("Please upload at least one file");
    return;
  }

  // UI: start processing
  status.innerText = "📤 Uploading files…";
  spinner.style.display = "block";
  button.disabled = true;

  const formData = new FormData();
  for (const file of fileInput.files) {
    formData.append("files", file);
  }

  let url = "/portfolio/beta";
  if (dateInput.value) {
    url += `?valuation_date=${dateInput.value}`;
  }

  try {
    status.innerText = "⚙️ Calculating portfolio beta…";

    const response = await fetch(url, {
      method: "POST",
      body: formData
    });

    const data = await response.json();

    if (!response.ok) {
      status.innerText = "❌ Calculation failed";
      alert(data.detail || "Error calculating beta");
      return;
    }

    renderResult(data);
    status.innerText = "✅ Calculation complete";

  } catch (err) {
    console.error(err);
    status.innerText = "❌ Server error";
    alert("Server error");

  } finally {
    spinner.style.display = "none";
    button.disabled = false;
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
