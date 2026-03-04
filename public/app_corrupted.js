
/* State Management */
const state = {
    projects: [],
    tasks: [],
    currentView: 'dashboard', // 'dashboard' | 'project'
    currentProjectId: null,
    chatHistory: [],
    isLoading: false,
    aiAvailable: false
};

/* --- API Helpers --- */
async function fetchState() {
    try {
        const [pRes, tRes, sRes] = await Promise.all([
            fetch('/api/projects').then(r => r.json()),
            fetch('/api/tasks').then(r => r.json()),
            fetch('/api/settings').then(r => r.json())
        ]);
        
        state.projects = pRes.projects || [];
        state.tasks = tRes.tasks || [];
        state.aiAvailable = !!sRes.openaiApiKey;
        updateAIStatus(state.aiAvailable);
        renderSidebar();
        renderMain();
    } catch (e) {
        console.error("Init Error", e);
    }
}

async function sendGlobalChat(message) {
    if (!message) return;
    
    // Optimistic UI
    const history = document.getElementById('global-chat-history');
    if(history) {
        appendMessage(history, 'user', message);
    }
    state.chatHistory.push({ role: 'user', content: message });
    
    try {
        const res = await fetch('/api/chat', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message })
        });
        const data = await res.json();
        
        if (history) {
            appendMessage(history, 'ai', data.reply);
        }
        state.chatHistory.push({ role: 'ai', content: data.reply });
        
        // Refresh state in case projects/tasks were created
        await fetchState(); 
        
    } catch (e) {
        if(history) appendMessage(history, 'ai', "Error connecting to AI.");
    }
}

/* --- Rendering --- */

function renderSidebar() {
    const list = document.querySelector('.nav-section:nth-of-type(2)'); // "Menu" or "Filters" -> Use custom
    // Actually, let's rebuild the sidebar project list
    const sidebar = document.getElementById('sidebar');
    
    // Clear old "Filters" section or reuse it
    // Let's find a specific container or create one.
    let container = document.getElementById('project-list-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'project-list-container';
        container.className = 'nav-section';
        container.innerHTML = '<div class="nav-label">Active Projects</div>';
        
        // Insert after the first nav section
        const first = sidebar.querySelector('.nav-section');
        if(first && first.nextSibling) {
            sidebar.insertBefore(container, first.nextSibling); 
        } else {
            sidebar.appendChild(container);
        }
    }
    
    // Populate
    const content = container.querySelector('div.nav-content') || document.createElement('div');
    content.className = 'nav-content';
    content.innerHTML = '';
    
    if (state.projects.length === 0) {
        content.innerHTML = '<div class="muted" style="padding:0.5rem;">No active projects</div>';
    }
    
    state.projects.forEach(p => {
        const btn = document.createElement('button');
        btn.className = `nav-item ${state.currentProjectId === p.id ? 'active' : ''}`;
        btn.textContent = p.name;
        btn.onclick = () => {
            state.currentProjectId = p.id;
            state.currentView = 'project';
            renderSidebar(); // Update active class
            renderMain();
        };
        content.appendChild(btn);
    });
    
    if(!container.querySelector('.nav-content')) container.appendChild(content);

    // Update Dashboard button
    const dashBtn = document.querySelector('[data-target="dashboard"]');
    if(dashBtn) {
        dashBtn.onclick = () => {
            state.currentProjectId = null;
            state.currentView = 'dashboard';
            renderSidebar();
            renderMain();
        };
        if(state.currentView === 'dashboard') dashBtn.classList.add('active');
        else dashBtn.classList.remove('active');
    }
}

function renderMain() {
    const container = document.getElementById('viewContainer');
    container.innerHTML = '';
    
    if (state.currentView === 'dashboard') {
        renderDashboard(container);
    } else if (state.currentView === 'project') {
        renderProjectView(container);
    }
}

function renderDashboard(container) {
    container.innerHTML = `
        <div style="max-width: 800px; margin: 0 auto; padding: 2rem; display: flex; flex-direction: column; height: 100%;">
            <header style="margin-bottom: 2rem;">
                <h1 style="font-size: 2rem; font-weight: 300; margin-bottom: 0.5rem;">Hello, Mark.</h1>
                <p style="color: var(--text-secondary);">System Online. ${state.projects.length} Active Projects.</p>
            </header>
            
            <div style="flex: 1; display: flex; flex-direction: column; background: var(--bg-card); border-radius: 8px; border: 1px solid var(--border); overflow: hidden;">
                <div id="global-chat-history" style="flex: 1; padding: 1.5rem; overflow-y: auto;">
                    <div class="chat-msg ai">Ready for instructions. I can create projects and tasks for you.</div>
                </div>
                <div style="padding: 1rem; border-top: 1px solid var(--border); background: var(--bg-input);">
                    <input type="text" id="global-input" placeholder="Type a command or request..." style="width: 100%; background: transparent; border: none; color: white; outline: none; font-size: 1rem;">
                </div>
            </div>
            
            <div style="margin-top: 2rem; display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 1rem;">
                ${state.projects.slice(0, 3).map(p => `
                    <div class="card" onclick="selectProject('${p.id}')" style="cursor: pointer; padding: 1rem; border: 1px solid var(--border); border-radius: 6px;">
                        <div style="font-weight: 600; margin-bottom: 0.25rem;">${p.name}</div>
                        <div style="font-size: 0.8rem; color: var(--text-secondary);">${p.type}</div>
                    </div>
                `).join('')}
            </div>
        </div>
    `;
    
    // Restore history
    const historyEl = container.querySelector('#global-chat-history');
    state.chatHistory.forEach(msg => appendMessage(historyEl, msg.role, msg.content));
    
    // Bind Input
    const input = container.querySelector('#global-input');
    input.focus();
    input.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            const val = input.value.trim();
            if(val) {
                input.value = '';
                sendGlobalChat(val);
            }
        }
    });
}

function renderProjectView(container) {
    const project = state.projects.find(p => p.id === state.currentProjectId);
    if (!project) return renderDashboard(container);
    
    container.innerHTML = `
        <div style="padding: 2rem; height: 100%; display: flex; flex-direction: column;">
            <header style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.5rem;">
                <div>
                    <h2 style="margin: 0; font-weight: 600;">${project.name}</h2>
                    <span style="color: var(--text-secondary); font-size: 0.9rem;">${project.type} &bull; ${project.status}</span>
                </div>
                <div>
                    <button class="btn btn--outline" onclick="backToDash()">Close</button>
                    ${project.workspacePath ? `<button class="btn btn--ghost" onclick="window.open('vscode://file/${normalizePath(project.workspacePath)}')">VS Code</button>` : ''}
                </div>
            </header>
            
            <div style="display: grid; grid-template-columns: 2fr 1fr; gap: 1.5rem; flex: 1; min-height: 0;">
                <!-- Left: Tasks -->
                <div class="panel" style="display: flex; flex-direction: column; overflow: hidden;">
                    <h3 style="padding: 1rem; border-bottom: 1px solid var(--border); margin: 0; background: var(--bg-sidebar);">Waitlist (Tasks)</h3>
                    <div id="project-tasks" style="flex: 1; overflow-y: auto; padding: 1rem;"></div>
                    <div style="padding: 0.5rem; border-top: 1px solid var(--border);">
                        <input type="text" placeholder="+ Add task..." id="quick-task-input" style="width: 100%; background: transparent; border: none; padding: 0.5rem; color: white;">
                    </div>
                </div>
                
                <!-- Right: Context -->
                <div class="panel" style="display: flex; flex-direction: column;">
                    <h3 style="padding: 1rem; border-bottom: 1px solid var(--border); margin: 0; background: var(--bg-sidebar);">Scratchpad</h3>
                    <textarea id="project-scratchpad" style="flex: 1; background: var(--bg-input); border: none; color: var(--text-primary); padding: 1rem; resize: none;">${project.scratchpad || ''}</textarea>
                    <button id="save-scratchpad" class="btn btn--primary full-width" style="border-radius: 0;">Save Notes</button>
                </div>
            </div>
        </div>
    `;
    
    renderProjectTasks(project, container.querySelector('#project-tasks'));
    
    // Bindings
    container.querySelector('#quick-task-input').addEventListener('keypress', async (e) => {
        if (e.key === 'Enter') {
            const title = e.target.value.trim();
            if (title) {
                e.target.value = '';
                await fetch('/api/tasks', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ task: { title, project: project.name, status: 'Next' } })
                });
                await fetchState(); // naive refresh
            }
        }
    });

    container.querySelector('#save-scratchpad').addEventListener('click', async () => {
         const text = container.querySelector('#project-scratchpad').value;
         await fetch(`/api/projects/${project.id}/scratchpad`, {
             method: 'PUT',
             headers: {'Content-Type': 'application/json'},
             body: JSON.stringify({ text })
         });
         alert('Saved');
    });
}

function renderProjectTasks(project, container) {
    const tasks = state.tasks.filter(t => t.project === project.name || t.project === project.id);
    container.innerHTML = '';
    
    if (tasks.length === 0) {
        container.innerHTML = '<div class="muted">No tasks pending.</div>';
        return;
    }
    
    // Sort
    const sorted = tasks.sort((a,b) => (a.priority||2) - (b.priority||2));
    
    sorted.forEach(t => {
        const el = document.createElement('div');
        el.className = 'task-row';
        el.style.display = 'flex';
        el.style.alignItems = 'center';
        el.style.padding = '0.75rem';
        el.style.borderBottom = '1px solid var(--border)';
        el.style.gap = '0.75rem';
        
        el.innerHTML = `
            <input type="checkbox" ${t.status === 'Done' ? 'checked' : ''}>
            <span style="flex: 1; ${t.status === 'Done' ? 'text-decoration: line-through; opacity: 0.5;' : ''}">${t.title}</span>
            <span class="badge" style="font-size: 0.7rem;">P${t.priority||2}</span>
        `;
        
        el.querySelector('input').addEventListener('change', async (e) => {
            const newStatus = e.target.checked ? 'Done' : 'Next';
            await fetch(`/api/tasks/${t.id}`, {
                method: 'PUT',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ patch: { status: newStatus } })
            });
            fetchState();
        });
        
        container.appendChild(el);
    });
}

function appendMessage(container, role, text) {
    const div = document.createElement('div');
    div.className = `chat-msg ${role}`;
    // Simple markdown-ish
    div.innerHTML = text.replace(/\n/g, '<br>');
    container.appendChild(div);
    container.scrollTop = container.scrollHeight;
}

function updateAIStatus(available) {
    const el = document.getElementById('aiStatus');
    if (!el) return;
    if (available) {
        el.className = 'status-indicator on';
        el.querySelector('.text').textContent = 'AI: Online';
    } else {
        el.className = 'status-indicator off';
        el.querySelector('.text').textContent = 'AI: Offline';
    }
}

function selectProject(id) {
    state.currentProjectId = id;
    state.currentView = 'project';
    renderSidebar();
    renderMain();
}

function backToDash() {
    state.currentProjectId = null;
    state.currentView = 'dashboard';
    renderSidebar();
    renderMain();
}

function normalizePath(p) {
    return p.replace(/\\/g, '/');
}

// Global Expose
window.selectProject = selectProject;
window.backToDash = backToDash;

// Start
init();
