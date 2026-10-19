let authToken = localStorage.getItem("model_mgr_token") || "";
let currentModels = [];

// DOM Elements
const loginScreen = document.getElementById("login-screen");
const dashboardScreen = document.getElementById("dashboard-screen");
const loginForm = document.getElementById("login-form");
const adminPasswordInput = document.getElementById("admin-password");
const btnLogout = document.getElementById("btn-logout");
const modelsTableBody = document.getElementById("models-table-body");
const searchInput = document.getElementById("search-input");
const statTotal = document.getElementById("stat-total-models");

// Modals
const addModal = document.getElementById("add-modal");
const btnOpenAdd = document.getElementById("btn-open-add");
const btnCloseModal = document.getElementById("btn-close-modal");
const btnCancelModal = document.getElementById("btn-cancel-modal");
const addModelForm = document.getElementById("add-model-form");
const newModelId = document.getElementById("new-model-id");
const keyType = document.getElementById("key-type");
const customKeyGroup = document.getElementById("custom-key-group");
const customKeyVal = document.getElementById("custom-key-val");

const testModal = document.getElementById("test-modal");
const btnCloseTestModal = document.getElementById("btn-close-test-modal");
const btnCloseTestBtn = document.getElementById("btn-close-test-btn");
const testSpinner = document.getElementById("test-spinner");
const testResultBox = document.getElementById("test-result-box");
const testStatusBadge = document.getElementById("test-status-badge");
const testLatency = document.getElementById("test-latency");
const testMessage = document.getElementById("test-message");

// API Request Helper
async function apiCall(path, method = "GET", body = null) {
  const headers = { "Content-Type": "application/json" };
  if (authToken) headers["Authorization"] = `Bearer ${authToken}`;
  const options = { method, headers };
  if (body) options.body = JSON.stringify(body);

  const res = await fetch(path, options);
  if (res.status === 401 && path !== "/api/auth/login") {
    logout();
    throw new Error("Phiên làm việc hết hạn. Vui lòng đăng nhập lại.");
  }
  const data = await res.json();
  if (!res.ok) throw new Error(data.detail || data.message || "Lỗi yêu cầu API");
  return data;
}

// Toast Notification
function showToast(msg, type = "success") {
  const container = document.getElementById("toast-container");
  const t = document.createElement("div");
  t.className = `toast toast-${type}`;
  t.innerText = msg;
  container.appendChild(t);
  setTimeout(() => t.remove(), 3500);
}

// Authentication
loginForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  const pwd = adminPasswordInput.value.trim();
  try {
    const res = await apiCall("/api/auth/login", "POST", { password: pwd });
    authToken = res.token;
    localStorage.setItem("model_mgr_token", authToken);
    showToast("Đăng nhập thành công!", "success");
    initDashboard();
  } catch (err) {
    showToast(err.message, "error");
  }
});

function logout() {
  authToken = "";
  localStorage.removeItem("model_mgr_token");
  dashboardScreen.style.display = "none";
  loginScreen.style.display = "flex";
  adminPasswordInput.value = "";
}

btnLogout.addEventListener("click", logout);

// Load & Render Models
async function fetchModels() {
  try {
    const res = await apiCall("/api/models");
    currentModels = res.models || [];
    renderTable(currentModels);
  } catch (err) {
    showToast(err.message, "error");
  }
}

function renderTable(models) {
  statTotal.innerText = models.length;
  modelsTableBody.innerHTML = "";
  if (!models.length) {
    modelsTableBody.innerHTML = `<tr><td colspan="5" style="text-align:center; padding:32px; color:var(--text-muted);">Chưa có model nào. Hãy nhấn "+ Thêm Model Mới" để bắt đầu!</td></tr>`;
    return;
  }

  models.forEach((m) => {
    const tr = document.createElement("tr");
    let statusBadge = `<span class="badge badge-secondary">⚪ No Key</span>`;
    if (m.has_key) {
      statusBadge = `<span class="badge badge-success">🟢 Active</span>`;
    } else if (m.is_dummy) {
      statusBadge = `<span class="badge badge-warning">🟡 Key giả (.env trống)</span>`;
    }

    tr.innerHTML = `
      <td><span class="code-text">${m.model_id}</span></td>
      <td><span class="badge badge-accent">${m.provider}</span></td>
      <td><code>${m.masked_key}</code></td>
      <td>${statusBadge}</td>
      <td>
        <button class="btn btn-test-sm" onclick="runTest('${m.model_id}')">⚡ Test</button>
        ${!m.is_system ? `<button class="btn btn-danger-sm" onclick="removeModel('${m.model_id}')">🗑️ Xóa</button>` : `<span class="badge badge-accent">System</span>`}
      </td>
    `;
    modelsTableBody.appendChild(tr);
  });
}

// Search Filter
searchInput.addEventListener("input", (e) => {
  const q = e.target.value.toLowerCase();
  const filtered = currentModels.filter(m => m.model_id.toLowerCase().includes(q) || m.provider.toLowerCase().includes(q));
  renderTable(filtered);
});

// Add Model Handlers
btnOpenAdd.addEventListener("click", () => {
  addModal.style.display = "flex";
  newModelId.value = "";
  customKeyVal.value = "";
  keyType.value = "gemini";
  customKeyGroup.style.display = "none";
});

const closeModal = () => { addModal.style.display = "none"; };
btnCloseModal.addEventListener("click", closeModal);
btnCancelModal.addEventListener("click", closeModal);

keyType.addEventListener("change", (e) => {
  customKeyGroup.style.display = e.target.value === "custom" ? "block" : "none";
});

document.querySelectorAll(".chip").forEach((chip) => {
  chip.addEventListener("click", () => {
    const name = chip.getAttribute("data-name");
    newModelId.value = name;
    if (name.includes("gemini")) keyType.value = "gemini";
    else if (name.includes("gpt")) keyType.value = "openai";
    customKeyGroup.style.display = keyType.value === "custom" ? "block" : "none";
  });
});

addModelForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  const mId = newModelId.value.trim();
  const kType = keyType.value;
  const cKey = customKeyVal.value.trim();

  try {
    const res = await apiCall("/api/models", "POST", { model_id: mId, key_type: kType, custom_key: cKey });
    showToast(res.message, "success");
    closeModal();
    fetchModels();
  } catch (err) {
    showToast(err.message, "error");
  }
});

// Delete Model
window.removeModel = async (modelId) => {
  if (!confirm(`Bạn có chắc muốn xóa model '${modelId}' khỏi Redis không?`)) return;
  try {
    const res = await apiCall(`/api/models/${modelId}`, "DELETE");
    showToast(res.message, "success");
    fetchModels();
  } catch (err) {
    showToast(err.message, "error");
  }
};

// Test Model
window.runTest = async (modelId) => {
  testModal.style.display = "flex";
  testSpinner.style.display = "block";
  testResultBox.style.display = "none";
  document.getElementById("test-modal-title").innerText = `🧪 Test: ${modelId}`;

  try {
    const data = await apiCall(`/api/models/${modelId}/test`, "POST");
    const r = data.result;
    testSpinner.style.display = "none";
    testResultBox.style.display = "block";
    testLatency.innerText = `${r.latency_ms} ms`;
    testMessage.innerText = r.message;

    if (r.success) {
      testStatusBadge.className = "badge badge-success";
      testStatusBadge.innerText = "✓ KẾT NỐI TỐT";
    } else {
      testStatusBadge.className = "badge badge-danger";
      testStatusBadge.innerText = "✗ THẤT BẠI";
    }
  } catch (err) {
    testSpinner.style.display = "none";
    testResultBox.style.display = "block";
    testStatusBadge.className = "badge badge-danger";
    testStatusBadge.innerText = "✗ LỖI KẾT NỐI";
    testMessage.innerText = err.message;
  }
};

const closeTestModal = () => { testModal.style.display = "none"; };
btnCloseTestModal.addEventListener("click", closeTestModal);
btnCloseTestBtn.addEventListener("click", closeTestModal);

document.getElementById("btn-refresh").addEventListener("click", fetchModels);

// Init Dashboard
function initDashboard() {
  loginScreen.style.display = "none";
  dashboardScreen.style.display = "block";
  fetchModels();
}

// Auto-login check
if (authToken) initDashboard();
