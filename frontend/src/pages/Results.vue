<script setup lang="ts">
import {ref,computed} from 'vue';
import * as api from '../api/runs';
import {useResource} from '../hooks/useResource';
import {user} from '../stores/session';
import {refreshResources} from '../stores/resources';
import PageTitle from '../components/common/PageTitle.vue';
import Notice from '../components/common/Notice.vue';
import StatusBadge from '../components/common/StatusBadge.vue';
import EmptyState from '../components/common/EmptyState.vue';
import PatternTable from '../components/PatternTable.vue';
import MedoidPanel from '../components/MedoidPanel.vue';
import AlignmentView from '../components/AlignmentView.vue';
const{data,error}=useResource(api.list,[]);
const id=ref(sessionStorage.getItem('journey-selected-run')||'run-baseline');
const run=computed(()=>data.value.find(r=>r.id===id.value)||data.value[0]);
const recoverError=ref('');
const waitingText=(r:any)=>r.status==='recovering'
 ?`上次执行在处理中断，已排队等待自动恢复（第 ${r.attempt??1} 次尝试），恢复后仍按入队时的旅程数据、编码册与方案重算。`
 :'独立worker正在挖掘与计算编辑距离，每3秒刷新。';
async function recoverNow(){try{recoverError.value='';await api.recover();refreshResources()}catch(e:any){recoverError.value=e.message}}
</script>
<template>
 <PageTitle chapter="05" title="读懂常见顺序，也看见不同的离开" description="支持度按旅程去重计数。编辑对齐给出最近成功路径的最少改动，属于描述性参照，不保证提高转化。"/>
 <Notice :error="error"/>
 <template v-if="run">
  <div class="toolbar">
   <label>计算记录<select aria-label="计算记录" v-model="id"><option v-for="r in data" :key="r.id" :value="r.id">{{r.id}} · {{r.status}}</option></select></label>
   <StatusBadge :status="run.status"/>
   <small v-if="run.status==='running'||run.status==='recovering'" class="run-attempt">第 {{run.attempt??1}} 次尝试 · 入队快照固定口径</small>
  </div>
  <template v-if="run.status==='completed'">
   <section class="paper">
    <div class="section-intro"><h2>频繁有序子序列</h2><p>{{run.result.total_patterns}}个模式 · 最低{{run.result.min_count}}条旅程 · 候选{{run.result.candidate_patterns_evaluated}}<span v-if="run.result.patterns_truncated"> · 仅展示前200个</span></p></div>
    <PatternTable :result="run.result" :limit="18"/>
    <small>本页预览前18模式，全部展示范围内模式随CSV导出。</small>
   </section>
   <section class="paper"><h2>按频数加权的代表路径</h2><MedoidPanel :result="run.result"/><p class="annotation">{{run.result.unique_paths}}种唯一序列 · 缓存距离对{{run.result.distance_cache_pairs}}。medoid平局按触点编码字典序。</p></section>
   <section class="paper"><h2>退出路径与最近成功路径的编辑对齐</h2><AlignmentView :result="run.result"/></section>
   <aside class="method-note"><h3>解释边界</h3><p v-for="x in run.result.limitations" :key="x">{{x}}</p><small>SHA256 {{run.result.checksum}}</small></aside>
  </template>
  <section v-else-if="run.status==='failed'" class="paper">{{run.error}}</section>
  <section v-else class="paper run-pending" :class="run.status">
   <span class="pending-dot" aria-hidden="true"></span>
   <div>
    <strong>{{run.status==='recovering'?'等待恢复':'处理中'}}</strong>
    <p>{{waitingText(run)}}</p>
    <button v-if="user?.role!=='viewer'" class="secondary" @click="recoverNow">重新排队等待恢复</button>
    <small v-if="recoverError" class="recover-error">{{recoverError}}</small>
   </div>
  </section>
 </template>
 <EmptyState v-else text="保存方案后运行，真实结果会显示在这里。"/>
</template>
