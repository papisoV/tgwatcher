
const API='';
let currentChat=null,currentPage=1,pageSize=50,totalMessages=0,crawlRunning=false,expandedRow=null;
let phoneCodeHash=null,allDialogs=[];
let authToken=localStorage.getItem('tgwatcher_token')||'';
let sseConnected=false,fallBackInterval=null,searchTimer=null;
let _loginCheckPending=false,_connCheckPending=false;

const CHART_COLORS=['#00e5ff','#00ff88','#ffb300','#a855f7','#f472b6','#60a5fa','#2dd4bf','#ff3d71'];

function _isLightTheme(){return document.documentElement.getAttribute('data-theme')==='light'}
function _chartColors(){
  const light=_isLightTheme();
  return{
    grid:light?'#d5d9e0':'#1a1f2e',
    tick:light?'#718096':'#6b7a8d',
    legend:light?'#4a5568':'#a0aec0',
    tooltipBg:light?'#ebedf2':'#141a26',
    tooltipTitle:light?'#1a2030':'#e8ecf1',
    tooltipBody:light?'#4a5568':'#a0aec0',
    border:light?'rgba(0,0,0,.08)':'rgba(255,255,255,.06)',
  };
}
function _chartOpts(){
  const c=_chartColors();
  return{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:c.legend,font:{family:"'Inter'",size:11},boxWidth:12}},tooltip:{backgroundColor:c.tooltipBg,titleColor:c.tooltipTitle,bodyColor:c.tooltipBody,borderColor:c.border,borderWidth:1}},interaction:{intersect:false,mode:'index'}};
}
function _chartScales(){
  const c=_chartColors();
  return{x:{grid:{color:c.grid,lineWidth:1},ticks:{color:c.tick,maxTicksLimit:10,font:{family:"'JetBrains Mono'",size:10}},border:{color:c.border}},y:{grid:{color:c.grid,lineWidth:1},ticks:{color:c.tick,font:{family:"'JetBrains Mono'",size:10}},border:{color:c.border}}};
}

async function api(path,opts={}){
  const headers={'Content-Type':'application/json',...opts.headers};
  if(authToken)headers['Authorization']='Bearer '+authToken;
  try{const ctrl=new AbortController();const timer=setTimeout(()=>ctrl.abort(),15000);
    const r=await fetch(API+path,{headers,...opts,signal:ctrl.signal});clearTimeout(timer);
    if(r.status===401){authToken='';localStorage.removeItem('tgwatcher_token');location.reload();return null}
    return await r.json()}catch(e){if(e.name==='AbortError')showToast('请求超时 — 请检查服务器','error');else console.error('API error:',e);return null}
}
function esc(s){if(!s)return'';const d=document.createElement('div');d.textContent=s;return d.innerHTML}
function fmtTime(iso){if(!iso)return'-';const d=new Date(iso+'Z');if(isNaN(d))return iso;const y=d.getFullYear(),m=String(d.getMonth()+1).padStart(2,'0'),day=String(d.getDate()).padStart(2,'0'),h=String(d.getHours()).padStart(2,'0'),mi=String(d.getMinutes()).padStart(2,'0');return `${y}-${m}-${day} ${h}:${mi}`}
function fmtTimeShort(iso){if(!iso)return'--:--';const d=new Date(iso+'Z');if(isNaN(d))return iso;return String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0')}

// ===== LOGIN =====
function showLoginStep(step){for(let i=0;i<=3;i++)document.getElementById('loginStep'+i).classList.toggle('active',i===step)}

function setAuthToken(){
  const t=document.getElementById('authTokenInput').value.trim();
  if(!t){document.getElementById('loginError0').textContent='请输入 Token';return}
  authToken=t;localStorage.setItem('tgwatcher_token',t);checkLogin();
}

async function checkLogin(){
  if(!authToken){
    // Try auto-login from localhost bootstrap endpoint first
    try{
      const r=await fetch(API+'/api/auth/bootstrap',{headers:{'Content-Type':'application/json'}});
      if(r.ok){
        const j=await r.json();
        if(j&&j.token){
          authToken=j.token;localStorage.setItem('tgwatcher_token',j.token);
        }
      }
    }catch(e){/* network error — fall back to manual entry */}
    if(!authToken){document.getElementById('loginOverlay').style.display='flex';return}
  }
  if(_loginCheckPending)return;_loginCheckPending=true;
  try{
    const r=await api('/api/login/status');
    if(!r){document.getElementById('loginOverlay').style.display='flex';return}
    if(r.error==='Unauthorized'){authToken='';localStorage.removeItem('tgwatcher_token');showLoginStep(0);document.getElementById('loginError0').textContent='Token 无效';document.getElementById('loginOverlay').style.display='flex';return}
    if(r.logged_in){document.getElementById('loginOverlay').style.display='none';document.getElementById('app').style.display='flex';initApp();return}
    showLoginStep(1);document.getElementById('loginOverlay').style.display='flex';
  }finally{_loginCheckPending=false}
}

async function sendCode(){
  document.getElementById('loginError1').textContent='';document.getElementById('btnSendCode').disabled=true;document.getElementById('btnSendCode').textContent='发送中...';
  const r=await api('/api/login',{method:'POST',body:JSON.stringify({})});
  document.getElementById('btnSendCode').disabled=false;document.getElementById('btnSendCode').textContent='发送验证码';
  if(!r){document.getElementById('loginError1').textContent='网络错误';return}
  if(r.status==='code_sent'){phoneCodeHash=r.phone_code_hash;showLoginStep(2);document.getElementById('loginCode').focus();document.getElementById('btnResendCode').style.display='inline'}
  else if(r.status==='already_logged_in'){document.getElementById('loginOverlay').style.display='none';document.getElementById('app').style.display='flex';initApp()}
  else document.getElementById('loginError1').textContent=r.error||'发送失败';
}

async function verifyCode(){
  const code=document.getElementById('loginCode').value.trim();
  if(!code){document.getElementById('loginError2').textContent='请输入验证码';return}
  document.getElementById('loginError2').textContent='';document.getElementById('btnVerify').disabled=true;document.getElementById('btnVerify').textContent='验证中...';
  const r=await api('/api/login',{method:'POST',body:JSON.stringify({code,phone_code_hash:phoneCodeHash})});
  document.getElementById('btnVerify').disabled=false;document.getElementById('btnVerify').textContent='验证';
  if(!r){document.getElementById('loginError2').textContent='网络错误';return}
  if(r.status==='logged_in')showLoginStep(3);
  else{document.getElementById('loginError2').textContent=r.error||'验证失败';document.getElementById('btnResendCode').style.display='inline'}
}

function afterLogin(){document.getElementById('loginOverlay').style.display='none';document.getElementById('app').style.display='flex';initApp()}
function showTokenHelp(){const h=document.getElementById('tokenHelp');h.style.display=h.style.display==='none'?'block':'none'}

async function resendCode(){
  document.getElementById('loginError2').textContent='';document.getElementById('btnResendCode').disabled=true;document.getElementById('btnResendCode').textContent='重新发送中...';
  const r=await api('/api/login',{method:'POST',body:JSON.stringify({})});
  document.getElementById('btnResendCode').disabled=false;document.getElementById('btnResendCode').textContent='重新发送';
  if(!r){document.getElementById('loginError2').textContent='网络错误';return}
  if(r.status==='code_sent'){phoneCodeHash=r.phone_code_hash;document.getElementById('loginCode').value='';document.getElementById('loginCode').focus()}
  else if(r.status==='already_logged_in'){document.getElementById('loginOverlay').style.display='none';document.getElementById('app').style.display='flex';initApp()}
  else document.getElementById('loginError2').textContent=r.error||'发送失败';
}

// ===== GROUP PICKER =====
async function openGroupModal(){
  const list=document.getElementById('groupList');
  list.innerHTML='<div style="padding:16px;text-align:center;color:var(--text-2)">加载中...</div>';
  document.getElementById('groupModal').style.display='flex';
  const r=await api('/api/dialogs');
  if(!r||r.error){list.innerHTML='<div style="padding:16px;text-align:center;color:var(--red)">'+esc(r?.error||'加载失败')+'</div>';return}
  allDialogs=r;const cfg=await api('/api/config');const existingIds=new Set((cfg?.groups||[]).map(g=>g.id||g.username));
  list.innerHTML=r.map(d=>{
    const sel=existingIds.has(d.id)?' selected':'';
    const kind=d.is_channel?'频道':'群组';
    return `<div class="modal-item${sel}" data-id="${d.id}" data-title="${esc(d.title)}" data-username="${esc(d.username||'')}" onclick="this.classList.toggle('selected')">
      <div class="modal-item-check">✓</div>
      <div class="modal-item-info"><div class="modal-item-title">${esc(d.title)}</div>
      <div class="modal-item-sub">${kind}${d.username?' · @'+d.username:''}${d.members?' · '+d.members+'人':''}</div></div></div>`;
  }).join('');
}
function closeGroupModal(){document.getElementById('groupModal').style.display='none'}
async function saveGroups(){
  const items=document.querySelectorAll('#groupList .modal-item.selected');
  // Preserve auto_catchup from existing config
  const cfg=await api('/api/config');
  const existingMap={};
  (cfg?.groups||[]).forEach(g=>{if(g.id)existingMap[g.id]=g.auto_catchup||false});
  const groups=Array.from(items).map(el=>{
    const id=parseInt(el.dataset.id);
    return{id,name:el.dataset.title,username:el.dataset.username||undefined,auto_catchup:existingMap[id]||false};
  }).filter(g=>g.id);
  if(!groups.length){showToast('请至少选择一个群组','error');return}
  const r=await api('/api/config/groups',{method:'PUT',body:JSON.stringify({groups})});
  if(r&&r.status==='updated'){closeGroupModal();loadChats();loadGroupsView()}else showToast('保存失败','error');
}

// ===== CHATS =====
async function loadChats(){
  const chats=await api('/api/chats');if(!chats)return;
  document.getElementById('chatList').innerHTML=chats.map(c=>{
    const typeTag=c.chat_type?`<span style="font-size:10px;color:var(--text-3);margin-left:4px">${c.chat_type==='channel'?'频道':'群组'}</span>`:'';
    return `<div class="chat-item${currentChat===c.chat_id?' active':''}" data-chat-id="${c.chat_id}" onclick="filterChat(${c.chat_id})">
      <div class="chat-dot"></div>
      <div class="chat-info"><div class="chat-name">${esc(c.chat_title||'ID:'+c.chat_id)}${typeTag}</div>
      <div class="chat-count">${c.msg_count}</div></div>
      <span class="chat-remove" onclick="event.stopPropagation();removeGroup(${c.chat_id})" title="删除">×</span></div>`;
  }).join('');
}

async function removeGroup(chatId){
  if(!confirm('确定删除该群组及其所有消息数据？'))return;
  const r=await api('/api/config/groups/'+chatId,{method:'DELETE'});
  if(r&&r.status==='removed'){loadChats();loadGroupsView();if(currentChat===chatId){currentChat=null;loadMessages(1)}showToast('已删除 '+r.messages_deleted+' 条消息','success')}
  else showToast(r?.error||'删除失败','error');
}

// ===== MESSAGES =====
function debounceSearch(){clearTimeout(searchTimer);searchTimer=setTimeout(searchMessages,300)}

async function loadSenders(){
  if(!currentChat)return;const senders=await api('/api/senders?chat_id='+currentChat);if(!senders)return;
  const sel=document.getElementById('senderFilter');const cur=sel.value;
  sel.innerHTML='<option value="">全部发送者</option>'+senders.map(s=>`<option value="${s.sender_id}"${s.sender_id==cur?' selected':''}>${esc(s.sender_name||'ID:'+s.sender_id)} (${s.msg_count})</option>`).join('');
}

async function loadMessages(page=1){
  currentPage=page;const params=new URLSearchParams({page,size:pageSize});
  if(currentChat)params.set('chat_id',currentChat);
  const kw=document.getElementById('searchKeyword').value.trim();if(kw)params.set('keyword',kw);
  const sid=document.getElementById('senderFilter').value;if(sid)params.set('sender_id',sid);
  const df=document.getElementById('searchDateFrom').value;if(df)params.set('date_from',df);
  const dt=document.getElementById('searchDateTo').value;if(dt)params.set('date_to',dt);
  const data=await api('/api/messages?'+params);if(!data)return;
  totalMessages=data.total;const msgs=data.messages||[];
  const tbody=document.getElementById('msgBody');
  if(!msgs.length){tbody.innerHTML='<tr><td colspan="4" style="text-align:center;padding:40px;color:var(--text-3)">暂无消息。请添加群组并开始爬取。</td></tr>'}
  else{tbody.innerHTML=msgs.map(m=>{
    const t=esc(m.text||'');const ts=t.length>80?t.slice(0,80)+'...':t;
    const fullDate=fmtTime(m.date);
    const today=new Date();const todayStr=today.getFullYear()+'-'+String(today.getMonth()+1).padStart(2,'0')+'-'+String(today.getDate()).padStart(2,'0');
    const msgDay=m.date?fmtTime(m.date).slice(0,10):'';
    const displayDate=msgDay===todayStr?fullDate.slice(11):fullDate;
    const med=m.media_type?{'photo':'图','video':'视频','document':'文档','sticker':'贴纸','audio':'音频','voice':'语音','webpage':'链接','contact':'联系人'}[m.media_type]||'媒体':(m.has_media?'文件':'');
    const medTag=med?`<span class="col-media">${med}</span>`:'';
    const editTag=m.is_edited?'<span style="color:var(--text-3);font-size:10px;margin-right:3px">已编辑</span>':'';
    const reply=m.reply_to_msg_id?`<span class="col-reply" onclick="event.stopPropagation();loadReply(${m.reply_to_msg_id},this)">↩${m.reply_to_msg_id}</span> `:'';
    const fwd=m.forward_from?`<span class="col-forward">转发: ${esc(m.forward_from)}</span> `:'';
    const chatTag=!currentChat&&m.chat_title?`<span class="col-chat-badge">${esc(m.chat_title)}</span> `:'';
    return `<tr data-id="${m.id}" onclick="toggleRow(this)">
      <td class="col-time" title="${fullDate}">${displayDate}</td>
      <td class="col-sender">${esc(m.sender_name||m.sender_username||'-')}</td>
      <td class="col-content">${chatTag}${editTag}${reply}${fwd}${ts}</td>
      <td>${medTag}</td></tr>`;
  }).join('')}
  renderPagination(data.total,data.page,data.page_size);
}

function toggleRow(tr){
  if(expandedRow===tr.dataset.id){tr.classList.remove('expanded');const f=tr.querySelector('.col-content-full');if(f)f.remove();const t=tr.querySelector('.col-content');if(t)t.style.display='';expandedRow=null;return}
  document.querySelectorAll('tr.expanded').forEach(r=>{r.classList.remove('expanded');const f=r.querySelector('.col-content-full');if(f)f.remove();const t=r.querySelector('.col-content');if(t)t.style.display=''});
  tr.classList.add('expanded');expandedRow=tr.dataset.id;
  const textEl=tr.querySelector('.col-content');const fullText=textEl.innerText;
  const div=document.createElement('div');div.className='col-content-full';div.textContent=fullText;
  textEl.style.display='none';textEl.parentNode.appendChild(div);
}

async function loadReply(msgId,el){
  const r=await api('/api/messages/'+msgId+'/reply');
  if(!r||r.error){el.textContent='未找到';return}
  const div=document.createElement('div');
  div.style.cssText='margin-top:4px;padding:6px;background:var(--bg-0);border:1px solid var(--border);font-size:var(--fs-xs);line-height:1.5;color:var(--text-1);max-width:400px';
  div.innerHTML='<b>'+esc(r.sender_name||'?')+'</b> <span style="color:var(--text-3)">'+fmtTime(r.date)+'</span><br>'+esc(r.text||'').slice(0,300);
  el.after(div);el.onclick=()=>div.remove();
}

function renderPagination(total,page,size){
  const pages=Math.ceil(total/size)||1;
  document.getElementById('pageInfo').textContent=`${total} 条 · ${page}/${pages} 页`;
  const btns=[];const start=Math.max(1,page-3);const end=Math.min(pages,page+3);
  if(page>1)btns.push(`<span class="page-btn" onclick="loadMessages(${page-1})">‹</span>`);
  for(let i=start;i<=end;i++)btns.push(`<span class="page-btn${i===page?' active':''}" onclick="loadMessages(${i})">${i}</span>`);
  if(page<pages)btns.push(`<span class="page-btn" onclick="loadMessages(${page+1})">›</span>`);
  document.getElementById('pageBtns').innerHTML=btns.join('');
}

function filterChat(chatId){
  currentChat=chatId;currentPage=1;
  document.querySelectorAll('.chat-all-item,.chat-item').forEach(el=>el.classList.remove('active'));
  if(chatId===null)document.querySelector('.chat-all-item').classList.add('active');
  else document.querySelectorAll('.chat-item').forEach(el=>{if(el.dataset.chatId==chatId)el.classList.add('active')});
  loadChats();loadMessages(1);loadSenders();
}
function searchMessages(){currentPage=1;loadMessages(1)}
function clearSearch(){document.getElementById('searchKeyword').value='';document.getElementById('senderFilter').value='';document.getElementById('searchDateFrom').value='';document.getElementById('searchDateTo').value='';searchMessages()}

// ===== EXPORT MODAL =====
let _exportPreviewTimer=null;

async function openExportModal(){
  const chats=await api('/api/chats');
  const sel=document.getElementById('exportGroupSelect');
  sel.innerHTML='<option value="">全部群组</option>';
  if(chats){
    chats.forEach(c=>{
      sel.innerHTML+=`<option value="${c.chat_id}">${esc(c.chat_title||'ID:'+c.chat_id)} (${c.msg_count})</option>`;
    });
  }
  if(currentChat){
    sel.value=currentChat;
  }
  setExportQuickDate('7d');
  document.getElementById('exportModal').style.display='flex';
}

function closeExportModal(){
  document.getElementById('exportModal').style.display='none';
}

function _localDateStr(d){
  const y=d.getFullYear(),m=String(d.getMonth()+1).padStart(2,'0'),day=String(d.getDate()).padStart(2,'0');
  return `${y}-${m}-${day}`;
}

function setExportQuickDate(preset){
  const now=new Date();
  const today=new Date(now.getFullYear(),now.getMonth(),now.getDate());
  let from,to;
  switch(preset){
    case 'today': from=today;to=today; break;
    case 'yesterday': from=new Date(today);from.setDate(from.getDate()-1);to=new Date(today);to.setDate(to.getDate()-1); break;
    case '7d': from=new Date(today);from.setDate(from.getDate()-6);to=today; break;
    case '30d': from=new Date(today);from.setDate(from.getDate()-29);to=today; break;
    case 'this_month': from=new Date(now.getFullYear(),now.getMonth(),1);to=today; break;
    case 'last_month': from=new Date(now.getFullYear(),now.getMonth()-1,1);to=new Date(now.getFullYear(),now.getMonth(),0); break;
    default: return;
  }
  document.getElementById('exportDateFrom').value=_localDateStr(from);
  document.getElementById('exportDateTo').value=_localDateStr(to);
  document.querySelectorAll('.export-quick-btn').forEach(b=>b.classList.toggle('active',b.dataset.preset===preset));
  updateExportPreview();
}

function clearExportQuickDate(){
  document.querySelectorAll('.export-quick-btn').forEach(b=>b.classList.remove('active'));
}

async function updateExportPreview(){
  clearTimeout(_exportPreviewTimer);
  _exportPreviewTimer=setTimeout(async()=>{
    const params=_buildExportQueryParams(1);
    const data=await api('/api/messages?'+params);
    const el=document.getElementById('exportPreview');
    if(data){
      const fmt=document.querySelector('input[name="exportFormat"]:checked')?.value||'json';
      el.textContent=`将导出 ${data.total} 条消息 (${fmt.toUpperCase()})`;
    }else{
      el.textContent='查询失败';
    }
  },300);
}

function _buildExportQueryParams(size){
  const params=new URLSearchParams();
  const chatId=document.getElementById('exportGroupSelect').value;
  if(chatId)params.set('chat_id',chatId);
  const df=document.getElementById('exportDateFrom').value;
  if(df)params.set('date_from',df);
  const dt=document.getElementById('exportDateTo').value;
  if(dt)params.set('date_to',dt);
  params.set('page','1');
  params.set('size',String(size||1));
  return params;
}

async function doExport(){
  const fmt=document.querySelector('input[name="exportFormat"]:checked')?.value||'json';
  const params=new URLSearchParams({format:fmt});
  const chatId=document.getElementById('exportGroupSelect').value;
  if(chatId)params.set('chat_id',chatId);
  const df=document.getElementById('exportDateFrom').value;
  if(df)params.set('date_from',df);
  const dt=document.getElementById('exportDateTo').value;
  if(dt)params.set('date_to',dt);
  if(!df&&!dt){
    showToast('请选择日期范围','error');return;
  }
  const r=await fetch(API+'/api/messages/export?'+params,{headers:{'Authorization':'Bearer '+authToken}});
  if(!r.ok){showToast('导出失败','error');return}
  const blob=await r.blob();
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);
  const ext=fmt==='markdown'?'md':fmt;
  a.download='tgwatcher_export.'+ext;a.click();
  closeExportModal();
  showToast('已导出 '+fmt.toUpperCase(),'success');
}

// ===== SIGNAL EXPORT MODAL =====
let _signalExportPreviewTimer=null;

async function openSignalExportModal(){
  const chats=await api('/api/chats');
  const sel=document.getElementById('signalExportGroupSelect');
  sel.innerHTML='<option value="">全部群组</option>';
  if(chats){
    chats.forEach(c=>{
      sel.innerHTML+=`<option value="${c.chat_id}">${esc(c.chat_title||'ID:'+c.chat_id)} (${c.msg_count})</option>`;
    });
  }
  setSignalExportQuickDate('7d');
  document.getElementById('signalExportModal').style.display='flex';
}

function closeSignalExportModal(){
  document.getElementById('signalExportModal').style.display='none';
}

function setSignalExportQuickDate(preset){
  const now=new Date();
  const today=new Date(now.getFullYear(),now.getMonth(),now.getDate());
  let from,to;
  switch(preset){
    case 'today': from=today;to=today; break;
    case 'yesterday': from=new Date(today);from.setDate(from.getDate()-1);to=new Date(today);to.setDate(to.getDate()-1); break;
    case '7d': from=new Date(today);from.setDate(from.getDate()-6);to=today; break;
    case '30d': from=new Date(today);from.setDate(from.getDate()-29);to=today; break;
    case 'this_month': from=new Date(now.getFullYear(),now.getMonth(),1);to=today; break;
    case 'last_month': from=new Date(now.getFullYear(),now.getMonth()-1,1);to=new Date(now.getFullYear(),now.getMonth(),0); break;
    default: return;
  }
  document.getElementById('signalExportDateFrom').value=_localDateStr(from);
  document.getElementById('signalExportDateTo').value=_localDateStr(to);
  document.querySelectorAll('#signalExportModal .export-quick-btn').forEach(b=>b.classList.toggle('active',b.dataset.preset===preset));
  updateSignalExportPreview();
}

function clearSignalExportQuickDate(){
  document.querySelectorAll('#signalExportModal .export-quick-btn').forEach(b=>b.classList.remove('active'));
}

async function updateSignalExportPreview(){
  clearTimeout(_signalExportPreviewTimer);
  _signalExportPreviewTimer=setTimeout(async()=>{
    const el=document.getElementById('signalExportPreview');
    const df=document.getElementById('signalExportDateFrom').value;
    const dt=document.getElementById('signalExportDateTo').value;
    if(!df&&!dt){el.textContent='请选择日期范围';return}
    const fmt=document.querySelector('input[name="signalExportFormat"]:checked')?.value||'json';
    // Use export endpoint with count_only to get count
    const params=new URLSearchParams();
    params.set('format','json');
    params.set('count_only','true');
    const chatId=document.getElementById('signalExportGroupSelect').value;
    if(chatId)params.set('chat_id',chatId);
    if(df)params.set('date_from',df);
    if(dt)params.set('date_to',dt);
    try{
      const data=await api('/api/signals/export?'+params);
      if(data && data.count!==undefined){
        el.textContent=`将导出 ${data.count} 条分析结果 (${fmt.toUpperCase()})`;
      }else if(data && data.error){
        el.textContent=data.error;
      }else{
        el.textContent='查询失败';
      }
    }catch(e){
      el.textContent='查询失败';
    }
  },300);
}

function _buildSignalExportParams(){
  const params=new URLSearchParams();
  const chatId=document.getElementById('signalExportGroupSelect').value;
  if(chatId)params.set('chat_id',chatId);
  const df=document.getElementById('signalExportDateFrom').value;
  if(df)params.set('date_from',df);
  const dt=document.getElementById('signalExportDateTo').value;
  if(dt)params.set('date_to',dt);
  const source=document.querySelector('input[name="signalExportSource"]:checked')?.value;
  if(source)params.set('llm_model',source);
  const filter=document.querySelector('input[name="signalExportFilter"]:checked')?.value;
  if(filter)params.set('is_signal',filter==='signal'?'true':'false');
  return params;
}

async function doSignalExport(){
  const fmt=document.querySelector('input[name="signalExportFormat"]:checked')?.value||'json';
  const params=_buildSignalExportParams();
  params.set('format',fmt);
  const df=document.getElementById('signalExportDateFrom').value;
  const dt=document.getElementById('signalExportDateTo').value;
  if(!df&&!dt){
    showToast('请选择日期范围','error');return;
  }
  const r=await fetch(API+'/api/signals/export?'+params,{headers:{'Authorization':'Bearer '+authToken}});
  if(!r.ok){showToast('导出失败','error');return}
  const blob=await r.blob();
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);
  const ext=fmt==='markdown'?'md':fmt==='sqlite'?'db':fmt;
  const prefix=fmt==='sqlite'?'tg_factors':'signals_export';
  a.download=prefix+'.'+ext;a.click();
  closeSignalExportModal();
  showToast('已导出信号分析 ('+fmt.toUpperCase()+')','success');
}

// ===== TABS =====
function switchTab(tab){
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.toggle('active',b.dataset.tab===tab));
  document.getElementById('viewMessages').classList.toggle('active',tab==='messages');
  document.getElementById('viewDashboard').classList.toggle('active',tab==='dashboard');
  document.getElementById('viewGroups').classList.toggle('active',tab==='groups');
  document.getElementById('viewSignal').classList.toggle('active',tab==='signal');
  if(tab==='dashboard')loadDashboardTab();
  if(tab==='groups')loadGroupsView();
  if(tab==='signal')loadSignalTab();
}

// ===== DASHBOARD =====
let trendChart=null,comparisonChart=null;

async function loadDashboardTab(){
  const s=await api('/api/stats');if(!s)return;
  const days=s.earliest_message&&s.latest_message?Math.max(1,Math.round((new Date(s.latest_message+'Z')-new Date(s.earliest_message+'Z'))/86400000)):1;
  const avg=s.total_messages?Math.round(s.total_messages/days):0;
  document.getElementById('kpiTotal').textContent=(s.total_messages||0).toLocaleString();
  document.getElementById('kpiChats').textContent=s.monitored_chats||'0';
  document.getElementById('kpiAvgDay').textContent=avg.toLocaleString();
  const cs=await api('/api/crawl/status');
  const lastCrawl=cs?.last_crawl_at||s.latest_message;
  document.getElementById('kpiLastCrawl').textContent=lastCrawl?fmtTimeShort(lastCrawl):'--:--';
  loadTrendChart();loadHeatmap();loadComparisonChart();
}

async function loadTrendChart(){
  const days=parseInt(document.getElementById('trendPeriod')?.value)||30;
  const d=await api('/api/stats/trend?days='+days);if(!d||!d.labels)return;
  const datasets=d.datasets.map((ds,i)=>({label:ds.chat_title,data:ds.data,borderColor:CHART_COLORS[i%CHART_COLORS.length],backgroundColor:CHART_COLORS[i%CHART_COLORS.length]+'18',fill:true,tension:0.3,pointRadius:0,borderWidth:1.5}));
  if(trendChart)trendChart.destroy();const el=document.getElementById('trendChart');if(!el)return;
  trendChart=new Chart(el,{type:'line',data:{labels:d.labels,datasets},options:{..._chartOpts(),scales:_chartScales()}});
}

async function loadHeatmap(){
  const d=await api('/api/stats/heatmap');if(!d||!d.data)return;
  const canvas=document.getElementById('heatmapCanvas');if(!canvas)return;
  const ctx=canvas.getContext('2d');
  const W=canvas.width=canvas.parentElement.clientWidth||600,H=canvas.height=180;
  const cellW=Math.floor(W/25),cellH=Math.floor(H/8),maxC=d.data.length?Math.max(1,...d.data.map(e=>e.count)):1;
  const light=_isLightTheme();
  const labelColor=light?'#718096':'#4a5568';
  const intenseTextColor=light?'#1a2030':'#e8ecf1';
  ctx.clearRect(0,0,W,H);
  const dayLabels=['日','一','二','三','四','五','六'];
  ctx.font='10px Inter';ctx.fillStyle=labelColor;
  for(let dow=0;dow<7;dow++)ctx.fillText(dayLabels[dow],2,dow*cellH+cellH/2+3);
  for(const e of d.data){
    const x=(e.hour+1)*cellW,y=e.dow*cellH,intensity=e.count/maxC;
    const fillColor=light?`rgba(0,119,182,${0.06+intensity*0.94})`:`rgba(0,229,255,${0.08+intensity*0.92})`;
    ctx.fillStyle=fillColor;ctx.fillRect(x,y,cellW-1,cellH-1);
    if(intensity>0.5){ctx.fillStyle=intenseTextColor;ctx.font='9px JetBrains Mono';ctx.fillText(e.count,x+2,y+cellH/2+3)}
  }
  ctx.font='9px JetBrains Mono';ctx.fillStyle=labelColor;
  for(let h=0;h<24;h+=2)ctx.fillText(h+'时',(h+1)*cellW,7*cellH+12);
}

async function loadComparisonChart(){
  const d=await api('/api/stats/comparison');if(!d||!d.groups)return;
  const top=d.groups.slice(0,10);
  if(comparisonChart)comparisonChart.destroy();const el=document.getElementById('comparisonChart');if(!el)return;
  const c=_chartColors();
  comparisonChart=new Chart(el,{type:'bar',data:{labels:top.map(g=>(g.chat_title||g.chat_id+'').slice(0,20)),datasets:[
    {label:'消息数',data:top.map(g=>g.msg_count),backgroundColor:'#00e5ff33',borderColor:'#00e5ff',borderWidth:1},
    {label:'发送者',data:top.map(g=>g.active_senders),backgroundColor:'#00ff8833',borderColor:'#00ff88',borderWidth:1},
  ]},options:{..._chartOpts(),indexAxis:'y',scales:{x:{grid:{color:c.grid},ticks:{color:c.tick,font:{family:"'JetBrains Mono'",size:10}},border:{color:c.border}},y:{grid:{color:c.grid},ticks:{color:c.legend,font:{size:11}},border:{color:c.border}}}}});
}

// ===== GROUPS VIEW =====
async function loadGroupsView(){
  const chats=await api('/api/chats');if(!chats)return;
  document.getElementById('groupsBody').innerHTML=chats.map(c=>{
    const checked=c.auto_catchup?'checked':'';
    return `<tr>
    <td style="font-weight:500">${esc(c.chat_title||'ID:'+c.chat_id)}</td>
    <td style="font-family:var(--font-mono);font-size:var(--fs-xs);color:var(--text-2)">${c.chat_type==='channel'?'频道':c.chat_type||'-'}</td>
    <td class="col-num">${c.members||'-'}</td>
    <td class="col-num">${c.msg_count}</td>
    <td class="col-time">${fmtTime(c.last_msg_date)}</td>
    <td><label class="toggle-switch"><input type="checkbox" ${checked} onchange="toggleAutoCatchup(${c.chat_id},this.checked)"><span class="toggle-slider"></span></label></td>
    <td><button class="btn btn-danger" style="font-size:var(--fs-xs);padding:1px 6px" onclick="removeGroup(${c.chat_id})">✕</button></td>
  </tr>`}).join('');
}

async function toggleAutoCatchup(chatId,enabled){
  const r=await api('/api/config/groups/'+chatId+'/auto_catchup',{method:'PATCH',body:JSON.stringify({auto_catchup:enabled})});
  if(!r||r.error){showToast(r?.error||'更新失败','error');loadGroupsView()}
  else showToast(enabled?'已启用自动补爬':'已关闭自动补爬','success');
}

// ===== PURGE =====
async function purgeAllData(){
  if(!confirm('确定清空所有消息数据？此操作不可恢复！'))return;
  const r=await api('/api/data/purge',{method:'POST'});
  if(r&&r.status==='purged'){showToast('已清空 '+r.messages_deleted+' 条消息','success');loadChats();loadMessages(1);loadGroupsView()}
  else showToast('清空失败','error');
}

// ===== CRAWL CONTROL =====
function toggleCrawlMenu(){document.getElementById('crawlMenu').classList.toggle('show')}
document.addEventListener('click',e=>{if(!e.target.closest('.crawl-dd'))document.getElementById('crawlMenu').classList.remove('show')});

async function startCrawl(mode){
  document.getElementById('crawlMenu').classList.remove('show');
  if(mode==='date_range'){showDateRangePanel();return}
  const r=await api('/api/crawl/start',{method:'POST',body:JSON.stringify({mode})});
  if(r&&r.status==='started'){loadCrawlStatus();document.getElementById('crawlDetail').style.display='block';loadCrawlStatus()}
  else showToast(r?.error||'启动失败','error');
}

function showDateRangePanel(){
  document.getElementById('crawlMenu').classList.remove('show');
  const panel=document.getElementById('crawlDatePanel');
  panel.style.display='flex';
  const to=new Date();const from=new Date();from.setDate(from.getDate()-7);
  document.getElementById('crawlDateTo').value=_localDateStr(to);
  document.getElementById('crawlDateFrom').value=_localDateStr(from);
}

function hideDateRangePanel(){
  document.getElementById('crawlDatePanel').style.display='none';
}

async function startCrawlDateRange(){
  const from=document.getElementById('crawlDateFrom').value;
  const to=document.getElementById('crawlDateTo').value;
  if(!from||!to){showToast('请选择起止日期','error');return}
  if(from>to){showToast('起始日期不能晚于结束日期','error');return}
  const r=await api('/api/crawl/start',{method:'POST',body:JSON.stringify({mode:'date_range',offset_date:from,until_date:to})});
  if(r&&r.status==='started'){hideDateRangePanel();document.getElementById('crawlDetail').style.display='block';loadCrawlStatus()}else showToast(r?.error||'启动失败','error');
}

async function stopCrawl(){
  await api('/api/crawl/stop',{method:'POST'});
  showToast('正在停止爬取...','info');
  for(let i=0;i<10;i++){
    await new Promise(r=>setTimeout(r,1000));
    const s=await api('/api/crawl/status');
    if(s&&!s.running){updateCrawlUI(s);return}
  }
  loadCrawlStatus();
}

async function loadCrawlStatus(){const s=await api('/api/crawl/status');if(!s)return;updateCrawlUI(s)}

function updateCrawlUI(s){
  crawlRunning=s.running;
  const dot=document.getElementById('crawlBarDot');
  const status=document.getElementById('crawlBarStatus');
  const modeEl=document.getElementById('crawlBarMode');
  const detail=document.getElementById('crawlBarDetail');
  const counts=document.getElementById('crawlBarCounts');
  const meta=document.getElementById('crawlBarMeta');
  const progress=document.getElementById('crawlBarProgress');
  const btnStart=document.getElementById('btnStart');
  const btnStop=document.getElementById('btnStop');

  const modeLabels={'incremental':'增量','full':'全量','date_range':'日期范围','catchup':'补爬'};
  dot.className='crawl-bar__dot'+(s.running?' running':'')+(s.error?' error':'');
  if(s.running){
    status.textContent='爬取中';
    modeEl.textContent=modeLabels[s.mode]||s.mode;modeEl.style.display='';
    btnStart.style.display='none';btnStop.style.display='';
    hideDateRangePanel();

    const totalGroups=s.total_groups||1;
    const completedGroups=s.completed_groups||0;
    const groupIdx=s.current_group_index||0;
    const pct=Math.round(((completedGroups)+(s.current_group_fetched?0.5:0))/totalGroups*100);
    progress.style.width=Math.min(pct,100)+'%';
    progress.classList.add('running');

    // Detail: group progress
    const groupProgress=groupIdx>0?`[${groupIdx}/${totalGroups}] `:'';
    detail.textContent=groupProgress+(s.current_group||'—');

    // Counts: fetched/saved + per-group
    const totalFetched=s.total_fetched||0;
    const totalSaved=s.total_saved||0;
    const gf=s.current_group_fetched||0;
    const gs=s.current_group_saved||0;
    let countsText=`↑${totalFetched} ↓${totalSaved}`;
    if(gf>0)countsText+=`  (本组: ${gs}/${gf})`;
    counts.textContent=countsText;

    // Meta: speed + elapsed + ETA
    const speed=s.speed||0;
    const elapsed=s.elapsed_seconds||0;
    const eta=s.eta_seconds||0;
    const parts=[];
    if(elapsed>0)parts.push(_fmtDuration(elapsed));
    if(speed>0)parts.push(`${speed}条/分`);
    if(eta>0)parts.push('剩余'+_fmtDuration(eta));
    meta.textContent=parts.join(' · ');

    // Update detail panel
    updateCrawlDetail(s);
  }else{
    status.textContent=s.error?'错误':'空闲';
    modeEl.style.display='';btnStart.style.display='';btnStop.style.display='none';
    progress.style.width='0%';progress.classList.remove('running');
    if(s.error)detail.textContent='错误: '+s.error.slice(0,60);
    else if(s.last_crawl_at)detail.textContent='上次: '+fmtTime(s.last_crawl_at);
    else detail.textContent='暂无爬取数据';
    counts.textContent='';
    meta.textContent='';

    // Hide detail panel when crawl stops
    const dp=document.getElementById('crawlDetail');
    if(dp.style.display!=='none')updateCrawlDetail(s);
  }
  const connDot=document.getElementById('connDot');
  connDot.className='conn-dot'+(s.running?' running':'');
  const topDot=document.getElementById('crawlTopDot');
  if(topDot)topDot.className='conn-dot'+(s.running?' running':'')+(s.error?' error':'');
  const topStatus=document.getElementById('crawlTopStatus');
  if(topStatus)topStatus.textContent=status.textContent;
  const topMode=document.getElementById('crawlTopMode');
  if(topMode){topMode.textContent=modeEl.textContent;topMode.style.display=modeEl.style.display}
}

function updateCrawlDetail(s){
  const modeLabels={'incremental':'增量','full':'全量','date_range':'日期范围','catchup':'补爬','idle':'—'};
  document.getElementById('cdKpiMode').textContent=modeLabels[s.mode]||s.mode||'—';
  const gi=s.current_group_index||0,tg=s.total_groups||0;
  document.getElementById('cdKpiGroup').textContent=gi>0?`${gi}/${tg}`:'—';
  document.getElementById('cdKpiFetched').textContent=(s.total_fetched||0).toLocaleString();
  document.getElementById('cdKpiSaved').textContent=(s.total_saved||0).toLocaleString();
  const speed=s.speed||0;
  document.getElementById('cdKpiSpeed').textContent=speed>0?speed+'条/分':'—';
  document.getElementById('cdKpiElapsed').textContent=s.elapsed_seconds>0?_fmtDuration(s.elapsed_seconds):'—';
  document.getElementById('cdKpiEta').textContent=s.eta_seconds>0?_fmtDuration(s.eta_seconds):'—';

  const totalGroups=s.total_groups||1;
  const completed=s.completed_groups||0;
  const gf=s.current_group_fetched||0;
  const pct=Math.round((completed+(gf?0.5:0))/totalGroups*100);
  document.getElementById('cdProgressFill').style.width=Math.min(pct,100)+'%';
  document.getElementById('cdProgressPct').textContent=Math.min(pct,100)+'%';

  document.getElementById('cdCurrentGroup').textContent=s.current_group||'—';
  const gf2=s.current_group_fetched||0,gs2=s.current_group_saved||0;
  document.getElementById('cdCurrentCounts').textContent=gf2>0?`拉取 ${gf2} · 保存 ${gs2}`:'';
}

function toggleCrawlDetail(){
  const dp=document.getElementById('crawlDetail');
  const show=dp.style.display==='none';
  dp.style.display=show?'block':'none';
  if(show){
    const s=crawlRunning?null:null;
    loadCrawlStatus();
  }
}

function _fmtDuration(sec){
  if(sec<60)return sec+'秒';
  if(sec<3600)return Math.floor(sec/60)+'分'+(sec%60?sec%60+'秒':'');
  const h=Math.floor(sec/3600),m=Math.floor((sec%3600)/60);
  return h+'时'+(m?m+'分':'');
}

// ===== SSE =====
let autoPollState=[];let autoPollTimer=null;

function connectSSE(){
  if(!authToken)return;
  const es=new EventSource(API+'/api/events?token='+authToken);
  es.addEventListener('crawl_status',e=>{updateCrawlUI(JSON.parse(e.data))});
  es.addEventListener('new_messages',e=>{
    if(!currentChat||currentChat===JSON.parse(e.data).chat_id)loadMessages(currentPage);
  });
  es.addEventListener('crawl_error',e=>{showToast(e.data,'error')});
  es.addEventListener('signal_process_status',e=>{checkSignalStatus()});
  es.addEventListener('auto_poll_tick',e=>{try{const d=JSON.parse(e.data);refreshAutoPollUI()}catch(_){}});
  es.onopen=()=>{sseConnected=true;if(fallBackInterval){clearInterval(fallBackInterval);fallBackInterval=null};loadAutoPollState()};
  es.onerror=()=>{sseConnected=false;es.close();if(!fallBackInterval)fallBackInterval=setInterval(()=>{loadCrawlStatus()},30000);setTimeout(connectSSE,10000)};
}

// ===== AUTO POLL =====
async function loadAutoPollState(){
  const r=await api('/api/crawl/auto-poll');if(!r)return;
  autoPollState=r;refreshAutoPollUI();
  if(!autoPollTimer)autoPollTimer=setInterval(refreshAutoPollUI,1000);
}

function refreshAutoPollUI(){
  const el=document.getElementById('autoPollNext');if(!el)return;
  const active=autoPollState.filter(s=>s.enabled);
  if(active.length===0){el.style.display='none';return}
  // pick the soonest-expiring one
  const next=active.reduce((a,b)=>(a.remaining_seconds||0)<(b.remaining_seconds||0)?a:b);
  el.style.display='inline';
  const sec=Math.max(0,Math.floor(next.remaining_seconds||0));
  el.textContent='⟳ '+next.name+' '+sec+'s';
}

async function openAutoPollModal(){
  const list=document.getElementById('autoPollList');
  document.getElementById('autoPollModal').style.display='flex';
  list.innerHTML='<div style="padding:16px;text-align:center;color:var(--text-2)">加载中...</div>';
  await loadAutoPollState();
  if(!autoPollState||autoPollState.length===0){list.innerHTML='<div style="padding:16px;text-align:center;color:var(--text-2)">没有已监控的群组</div>';return}
  const fmtInterval=(i)=>i<60?i+'s':(i%60===0?(i/60)+'min':Math.round(i/60*10)/10+'min');
  list.innerHTML=autoPollState.map(s=>{
    return `<div class="modal-item" style="cursor:default">
      <div class="modal-item-check" style="background:${s.enabled?'var(--cyan)':'transparent'};color:${s.enabled?'var(--bg-0)':'transparent'};border-color:${s.enabled?'var(--cyan)':'var(--border)'}">✓</div>
      <div class="modal-item-info"><div class="modal-item-title">${esc(s.name)}</div><div class="modal-item-sub">${s.enabled?'已启用':'已关闭'}</div></div>
      <label class="toggle-switch" style="margin-right:10px"><input type="checkbox" ${s.enabled?'checked':''} onchange="updateAutoPoll(${s.chat_id},'enabled',this.checked)"><span class="toggle-slider"></span></label>
      <input type="number" class="filter-input" min="5" max="3600" step="1" value="${s.interval_seconds}" style="width:80px;text-align:right" onkeydown="if(event.key==='Enter'){this.blur()}" onblur="updateAutoPoll(${s.chat_id},'interval',this.value)" /><span style="margin-left:4px;color:var(--text-2);font-size:var(--fs-sm)">秒</span>
    </div>`;
  }).join('');
}

function closeAutoPollModal(){document.getElementById('autoPollModal').style.display='none'}

async function updateAutoPoll(chat_id,field,value){
  if(field==='interval'){
    const v=parseInt(value);
    if(!Number.isFinite(v)||v<5){showToast('间隔最少 5 秒','error');await loadAutoPollState();if(document.getElementById('autoPollModal').style.display==='flex')openAutoPollModal();return}
    if(v>3600){showToast('间隔最大 3600 秒','error');await loadAutoPollState();if(document.getElementById('autoPollModal').style.display==='flex')openAutoPollModal();return}
  }
  const body={};body[field==='enabled'?'enabled':'interval_seconds']=field==='enabled'?value===true:parseInt(value);
  const r=await api('/api/crawl/auto-poll/'+chat_id,{method:'PATCH',body:JSON.stringify(body)});
  if(!r||r.error){showToast(r?.error||'更新失败','error');return}
  showToast('已更新','success');
  await loadAutoPollState();
  if(document.getElementById('autoPollModal').style.display==='flex')openAutoPollModal();
}

// ===== TOAST =====
function showToast(msg,type='info'){
  const t=document.createElement('div');t.className='toast toast-'+type;t.textContent=msg;
  document.body.appendChild(t);setTimeout(()=>t.remove(),5000);
}

// ===== PANEL =====
function togglePanel(){
  const p=document.getElementById('leftPanel');p.classList.toggle('collapsed');
  const btn=p.querySelector('.panel-toggle');btn.textContent=p.classList.contains('collapsed')?'›':'‹';
}

// ===== THEME =====
function toggleTheme(){
  const html=document.documentElement;const isLight=html.getAttribute('data-theme')==='light';
  html.setAttribute('data-theme',isLight?'':'light');
  localStorage.setItem('tgwatcher_theme',isLight?'dark':'light');
  const txt=isLight?'☀':'🌙';
  document.getElementById('themeBtn').textContent=txt;
  const tb2=document.getElementById('themeBtn2');if(tb2)tb2.textContent=txt;
  if(document.getElementById('viewDashboard').classList.contains('active')){loadTrendChart();loadHeatmap();loadComparisonChart()}
}
(function(){const t=localStorage.getItem('tgwatcher_theme');if(t==='light'){document.documentElement.setAttribute('data-theme','light');document.addEventListener('DOMContentLoaded',()=>{document.getElementById('themeBtn').textContent='🌙';const tb2=document.getElementById('themeBtn2');if(tb2)tb2.textContent='🌙'})}})();

// ===== INIT =====
async function checkConnection(){
  if(_connCheckPending)return;_connCheckPending=true;
  const connDot=document.getElementById('connDot');
  try{const r=await api('/api/login/status');
    if(r&&r.logged_in)connDot.className='conn-dot connected';
    else connDot.className='conn-dot';
  }catch(e){connDot.className='conn-dot'}
  finally{_connCheckPending=false}
}

async function initApp(){
  if(typeof Chart==='undefined')showToast('Chart.js 加载失败 — 仪表盘不可用','error');
  await loadChats();await loadMessages(1);await loadCrawlStatus();
  checkConnection();connectSSE();
}

// ===== SIGNAL TAB =====
let signalTrendChart=null,signalEventChart=null;

async function loadSignalTab(){
  const s=await api('/api/signal/stats');if(!s)return;
  const total=s.total||0;
  const dir=s.direction||{};
  const bullishPct=total?Math.round((dir.bullish||0)/total*100):0;
  const bearishPct=total?Math.round((dir.bearish||0)/total*100):0;
  document.getElementById('signalKpiTotal').textContent=total.toLocaleString();
  document.getElementById('signalKpiBullish').textContent=bullishPct+'%';
  document.getElementById('signalKpiBearish').textContent=bearishPct+'%';
  document.getElementById('signalKpiUrgency').textContent=s.avg_magnitude?Number(s.avg_magnitude).toFixed(2):'-';
  loadSignalTrendChart();
  loadSignalEventChart(s.event_types||{});
  loadSignalTable();
  checkSignalStatus();
}

async function loadSignalTrendChart(){
  const d=await api('/api/signal/trend?days=30');if(!d||!d.trend)return;
  const labels=Object.keys(d.trend).sort();
  const avgDirection=labels.map(l=>(d.trend[l].avg_direction||0));
  const avgMagnitude=labels.map(l=>(d.trend[l].avg_magnitude||0));
  const counts=labels.map(l=>(d.trend[l].count||0));
  if(signalTrendChart)signalTrendChart.destroy();
  const el=document.getElementById('signalTrendChart');if(!el)return;
  signalTrendChart=new Chart(el,{type:'line',data:{labels,datasets:[
    {label:'方向均值',data:avgDirection,borderColor:'#00e5ff',backgroundColor:'#00e5ff18',fill:true,tension:0.3,pointRadius:0,borderWidth:1.5,yAxisID:'y'},
    {label:'幅度均值',data:avgMagnitude,borderColor:'#00ff88',backgroundColor:'#00ff8818',fill:true,tension:0.3,pointRadius:0,borderWidth:1.5,yAxisID:'y'},
    {label:'消息数',data:counts,borderColor:'#ffb300',backgroundColor:'#ffb30018',fill:false,tension:0.3,pointRadius:0,borderWidth:1,borderDash:[3,3],yAxisID:'y1'},
  ]},options:{responsive:true,maintainAspectRatio:false,scales:{x:{ticks:{color:'#6b7a8d',font:{size:10}},grid:{color:'#1a1f2e'}},y:{ticks:{color:'#6b7a8d',font:{size:10}},grid:{color:'#1a1f2e'},min:-1,max:1},y1:{position:'right',ticks:{color:'#6b7a8d',font:{size:10}},grid:{display:false},min:0}},plugins:{legend:{labels:{color:'#a0aec0',font:{size:11}}}}}});
}

function loadSignalEventChart(eventTypes){
  const labels=Object.keys(eventTypes);
  const data=labels.map(l=>eventTypes[l]);
  const colors=['#00e5ff','#00ff88','#ffb300','#ff3d71','#a855f7','#f472b6','#38bdf8','#6b7a8d'];
  if(signalEventChart)signalEventChart.destroy();
  const el=document.getElementById('signalEventChart');if(!el)return;
  signalEventChart=new Chart(el,{type:'doughnut',data:{labels,datasets:[{data,backgroundColor:colors.slice(0,labels.length),borderWidth:0}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'right',labels:{color:'#a0aec0',font:{size:11},padding:8}}}}});
}

async function loadSignalTable(){
  const d=await api('/api/signal/factors?page_size=50');if(!d||!d.items)return;
  const tbody=document.getElementById('signalTableBody');
  tbody.innerHTML=d.items.map(f=>{
    const dir=f.direction||0;
    const dirColor=dir>0.1?'#00ff88':dir<-0.1?'#ff3d71':'#6b7a8d';
    const dirLabel=dir>0.1?'利多':dir<-0.1?'利空':'中性';
    const symbols=JSON.parse(f.symbols||'[]').join(',');
    const text=(f.text||'').slice(0,50);
    const date=fmtTime(f.date);
    return `<tr><td style="white-space:nowrap">${date}</td><td style="color:${dirColor}">${dirLabel} ${dir.toFixed(2)}</td><td>${f.event_type||'-'}</td><td>${(f.magnitude||0).toFixed(2)}</td><td>${(f.urgency||0).toFixed(2)}</td><td>${(f.confidence||0).toFixed(2)}</td><td>${f.halflife_min||'-'}</td><td style="max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${text}</td></tr>`;
  }).join('');
}

async function startSignalProcess(){
  const r=await api('/api/signal/process','POST',{});if(!r)return;
  document.getElementById('btnSignalProcess').style.display='none';
  document.getElementById('btnSignalStop').style.display='';
  document.getElementById('signalProgress').textContent='处理中...';
}

async function stopSignalProcess(){
  await api('/api/signal/process/stop','POST');
  document.getElementById('btnSignalProcess').style.display='';
  document.getElementById('btnSignalStop').style.display='none';
}

async function checkSignalStatus(){
  const s=await api('/api/signal/process/status');if(!s)return;
  if(s.running){
    document.getElementById('btnSignalProcess').style.display='none';
    document.getElementById('btnSignalStop').style.display='';
    const pct=s.total?Math.round(s.processed/s.total*100):0;
    document.getElementById('signalProgress').textContent=`${s.processed||0}/${s.total||0} (${pct}%)`;
  }else{
    document.getElementById('btnSignalProcess').style.display='';
    document.getElementById('btnSignalStop').style.display='none';
    if(s.processed>0){
      document.getElementById('signalProgress').textContent=`完成: ${s.completed||0} 完成, ${s.failed||0} 失败, ${s.skipped||0} 跳过`;
    }else{
      document.getElementById('signalProgress').textContent='';
    }
  }
}
