import { useEffect, useState } from 'react'
import { Activity, ShieldAlert, Play, Square, Network, Database, LayoutDashboard } from 'lucide-react'

function App() {
  const [alerts, setAlerts] = useState<any[]>([])
  const [metrics, setMetrics] = useState({ tps: 0, lag: 0 })
  const [activeTab, setActiveTab] = useState('control')
  const [generatorRunning, setGeneratorRunning] = useState(false)

  useEffect(() => {
    // Connect to websockets through the current host (which proxies to fraud-api:8000)
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const host = window.location.host
    const metricsUrl = `${protocol}//${host}/ws/metrics`
    const alertsUrl = `${protocol}//${host}/ws/alerts`
    
    console.log('🔌 WebSocket URLs:', { metricsUrl, alertsUrl })
    
    const wsMetrics = new WebSocket(metricsUrl)
    const wsAlerts = new WebSocket(alertsUrl)
    
    wsMetrics.onopen = () => console.log('✅ Metrics WS connected')
    wsMetrics.onerror = (e) => console.error('❌ Metrics WS error:', e)
    wsMetrics.onmessage = (e) => { const d = JSON.parse(e.data); if (d.type === 'METRIC_UPDATE') setMetrics({ tps: d.tps, lag: d.consumer_lag }) }
    
    wsAlerts.onopen = () => console.log('✅ Alerts WS connected')
    wsAlerts.onerror = (e) => console.error('❌ Alerts WS error:', e)
    wsAlerts.onmessage = (e) => { const p = JSON.parse(e.data); if (p.type === 'NEW_ALERT') setAlerts((prev) => [p.data, ...prev].slice(0, 50)) }
    
    return () => { wsMetrics.close(); wsAlerts.close() }
  }, [])

  const handleControl = async (action: 'start' | 'stop') => {
    try {
      await fetch(`/api/v1/control/${action}`, { method: 'POST' })
      setGeneratorRunning(action === 'start')
    } catch (err) {
      console.error(err)
    }
  }

  return (
    <div className="min-h-screen bg-slate-50 font-sans text-slate-800">
      
      {/* Top Navigation */}
      <header className="bg-white border-b border-slate-200 shadow-sm sticky top-0 z-10">
        <div className="max-w-6xl mx-auto px-6 h-16 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <ShieldAlert className="text-blue-600" />
            <h1 className="text-lg font-bold tracking-tight text-slate-900">Fraud Intelligence Platform</h1>
          </div>
          <nav className="flex gap-4">
            <button onClick={() => setActiveTab('control')} className={`flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-md transition-colors ${activeTab === 'control' ? 'bg-blue-50 text-blue-700' : 'text-slate-600 hover:bg-slate-100'}`}><Activity size={16}/> Generator</button>
            <button onClick={() => setActiveTab('kafka')} className={`flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-md transition-colors ${activeTab === 'kafka' ? 'bg-blue-50 text-blue-700' : 'text-slate-600 hover:bg-slate-100'}`}><Network size={16}/> Kafka Stream</button>
            <button onClick={() => setActiveTab('dashboard')} className={`flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-md transition-colors ${activeTab === 'dashboard' ? 'bg-blue-50 text-blue-700' : 'text-slate-600 hover:bg-slate-100'}`}><LayoutDashboard size={16}/> Intelligence</button>
          </nav>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-6 py-8">
        
        {/* Screen 1: Generator Control */}
        {activeTab === 'control' && (
          <div className="bg-white p-8 rounded-xl shadow-sm border border-slate-200 max-w-2xl mx-auto">
            <h2 className="text-xl font-bold mb-6">Transaction Generator Control</h2>
            <p className="text-slate-500 mb-8">Start the simulated transaction stream. This will inject normal and fraudulent transactions into Kafka.</p>
            
            <div className="flex gap-4">
              <button 
                onClick={() => handleControl('start')}
                disabled={generatorRunning}
                className="flex-1 flex items-center justify-center gap-2 bg-green-600 hover:bg-green-700 disabled:opacity-50 text-white py-3 rounded-lg font-semibold transition-colors">
                <Play size={20}/> Start Stream
              </button>
              <button 
                onClick={() => handleControl('stop')}
                disabled={!generatorRunning}
                className="flex-1 flex items-center justify-center gap-2 bg-red-600 hover:bg-red-700 disabled:opacity-50 text-white py-3 rounded-lg font-semibold transition-colors">
                <Square size={20}/> Stop Stream
              </button>
            </div>
            
            <div className="mt-8 p-4 bg-slate-50 rounded-lg border border-slate-100 flex items-center justify-between">
              <span className="text-slate-600 font-medium">Status</span>
              {generatorRunning ? (
                <span className="flex items-center gap-2 text-green-600 font-bold"><span className="w-3 h-3 bg-green-500 rounded-full animate-pulse"></span> RUNNING</span>
              ) : (
                <span className="flex items-center gap-2 text-slate-400 font-bold"><span className="w-3 h-3 bg-slate-300 rounded-full"></span> STOPPED</span>
              )}
            </div>
          </div>
        )}

        {/* Screen 2: Kafka Flow Animation */}
        {activeTab === 'kafka' && (
          <div className="bg-white p-8 rounded-xl shadow-sm border border-slate-200">
            <div className="flex justify-between items-center mb-12">
              <h2 className="text-xl font-bold">Live Stream Observatory</h2>
              <div className="flex gap-6">
                <div className="text-center">
                  <p className="text-xs text-slate-500 uppercase tracking-wide">Live TPS</p>
                  <p className="text-2xl font-bold text-blue-600">{metrics.tps.toLocaleString()}</p>
                </div>
                <div className="text-center">
                  <p className="text-xs text-slate-500 uppercase tracking-wide">Consumer Lag</p>
                  <p className="text-2xl font-bold text-green-600">{metrics.lag}</p>
                </div>
              </div>
            </div>

            {/* Animation Container */}
            <div className="relative h-64 bg-slate-50 rounded-xl border border-slate-200 flex items-center justify-between px-16 overflow-hidden">
              <div className="z-10 flex flex-col items-center bg-white p-4 rounded-lg shadow-sm border border-slate-200">
                <Database className="text-slate-700 mb-2" size={32} />
                <span className="font-semibold text-sm">Generator</span>
              </div>
              
              {/* Animated Dots flowing right */}
              {generatorRunning && (
                <div className="absolute inset-0 flex items-center justify-center opacity-70">
                  <div className="w-full h-1 flex justify-between px-32 space-x-4 animate-[pulse_1s_ease-in-out_infinite]">
                    <div className="w-3 h-3 rounded-full bg-blue-500 animate-[bounce_1s_infinite]"></div>
                    <div className="w-3 h-3 rounded-full bg-blue-400 animate-[bounce_1.2s_infinite]"></div>
                    <div className="w-3 h-3 rounded-full bg-red-400 animate-[bounce_0.8s_infinite]"></div>
                    <div className="w-3 h-3 rounded-full bg-blue-500 animate-[bounce_1.5s_infinite]"></div>
                  </div>
                </div>
              )}

              <div className="z-10 flex flex-col items-center bg-blue-50 p-6 rounded-full shadow-md border-4 border-blue-200">
                <Network className="text-blue-700 mb-2" size={40} />
                <span className="font-bold text-blue-900">Apache Kafka</span>
              </div>

              <div className="z-10 flex flex-col items-center bg-white p-4 rounded-lg shadow-sm border border-slate-200">
                <Activity className="text-orange-500 mb-2" size={32} />
                <span className="font-semibold text-sm">Apache Flink</span>
              </div>
            </div>
            <p className="text-center text-slate-400 text-sm mt-4">Real-time event backbone distributing transactions to streaming jobs.</p>
          </div>
        )}

        {/* Screen 3: Intelligence Dashboard */}
        {activeTab === 'dashboard' && (
          <div className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
            <div className="p-6 border-b border-slate-200 bg-slate-50 flex justify-between items-center">
              <h2 className="text-lg font-bold">Real-Time Risk Intelligence</h2>
              <span className="bg-white border border-slate-200 px-3 py-1 rounded-full text-xs font-semibold text-slate-500 shadow-sm flex items-center gap-2">
                <span className="w-2 h-2 rounded-full bg-green-500 animate-pulse"></span>
                Monitoring Active Stream
              </span>
            </div>
            
            <div className="divide-y divide-slate-100">
              {alerts.length === 0 ? (
                <div className="p-12 text-center text-slate-400">
                  <p>No anomalies detected yet. Start the generator to see alerts.</p>
                </div>
              ) : (
                alerts.map((alert, idx) => {
                  const isHighRisk = alert.risk_score > 70
                  return (
                    <div key={idx} className="p-4 hover:bg-slate-50 transition-colors flex items-center justify-between">
                      <div className="flex items-center gap-4">
                        {/* Signal Dot */}
                        <div className={`w-3 h-3 rounded-full shadow-sm ${isHighRisk ? 'bg-red-500 animate-pulse' : 'bg-green-500'}`}></div>
                        
                        <div>
                          <p className="font-semibold text-slate-900 text-sm">{alert.transaction_id}</p>
                          <p className="text-slate-500 text-xs mt-1">{alert.reason}</p>
                        </div>
                      </div>
                      
                      <div className="flex items-center gap-6">
                        <span className={`px-2 py-1 rounded text-xs font-bold ${isHighRisk ? 'bg-red-100 text-red-700' : 'bg-green-100 text-green-700'}`}>
                          {isHighRisk ? 'HIGH RISK' : 'LOW RISK'}
                        </span>
                        <div className="text-right">
                          <p className="text-xs text-slate-400">Score</p>
                          <p className={`font-bold ${isHighRisk ? 'text-red-600' : 'text-slate-700'}`}>{alert.risk_score}</p>
                        </div>
                      </div>
                    </div>
                  )
                })
              )}
            </div>
          </div>
        )}

      </main>
    </div>
  )
}

export default App
