import { useEffect, useState } from 'react'
import { Activity, ShieldAlert } from 'lucide-react'

function App() {
  const [alerts, setAlerts] = useState<any[]>([])
  const [metrics, setMetrics] = useState({ tps: 0, lag: 0 })

  useEffect(() => {
    const wsMetrics = new WebSocket('ws://localhost:8000/ws/metrics')
    const wsAlerts = new WebSocket('ws://localhost:8000/ws/alerts')
    wsMetrics.onmessage = (e) => { const d = JSON.parse(e.data); if (d.type === 'METRIC_UPDATE') setMetrics({ tps: d.tps, lag: d.consumer_lag }) }
    wsAlerts.onmessage = (e) => { const p = JSON.parse(e.data); if (p.type === 'NEW_ALERT') setAlerts((prev) => [p.data, ...prev].slice(0, 20)) }
    return () => { wsMetrics.close(); wsAlerts.close() }
  }, [])

  return (
    <div className="p-8 max-w-5xl mx-auto font-mono">
      <h1 className="text-2xl font-bold mb-8 flex items-center gap-3"><ShieldAlert className="text-red-500" /> Fraud Intelligence Console</h1>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
        <div className="bg-slate-800 p-6 rounded-lg border border-slate-700">
          <h2 className="text-sm text-slate-300 font-semibold mb-4 flex items-center gap-2"><Activity className="text-blue-400" size={20}/> Metrics</h2>
          <p className="text-xs text-slate-500">Kafka TPS</p><p className="text-3xl font-bold">{metrics.tps}</p>
          <p className="text-xs text-slate-500 mt-4">Consumer Lag</p><p className="text-xl text-green-400 font-bold">{metrics.lag}</p>
        </div>
        <div className="col-span-2 space-y-3">
          <h2 className="text-sm text-slate-300 font-semibold mb-4">Live Alerts</h2>
          {alerts.map((a, i) => (
            <div key={i} className="bg-slate-800 border border-red-900 p-4 rounded-md flex justify-between">
              <div><span className="bg-red-900 text-red-400 text-xs px-2 py-1 rounded">{a.severity}</span> <span className="ml-2 text-sm">{a.transaction_id}</span><p className="text-sm text-slate-400 mt-2">{a.reason}</p></div>
              <div className="text-right"><p className="text-xs text-slate-500">Risk Score</p><p className="text-xl text-red-400 font-bold">{a.risk_score}</p></div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
export default App
