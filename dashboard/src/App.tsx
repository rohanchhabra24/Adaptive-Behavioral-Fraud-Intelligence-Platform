import { useEffect, useState } from 'react'
import { 
  Box, Container, AppBar, Toolbar, Typography, Tabs, Tab, Card, CardContent, 
  Button, Chip, Accordion, AccordionSummary, AccordionDetails, Divider 
} from '@mui/material'
import { 
  PlayArrow, Stop, Speed, Security, Insights, ExpandMore, Router, Storage, Hub
} from '@mui/icons-material'

function App() {
  const [alerts, setAlerts] = useState<any[]>([])
  const [metrics, setMetrics] = useState({ tps: 0, lag: 0 })
  const [activeTab, setActiveTab] = useState(0)
  const [generatorRunning, setGeneratorRunning] = useState(false)

  const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  const wsHost = window.location.host
  
  useEffect(() => {
    const wsMetrics = new WebSocket(`${wsProtocol}//${wsHost}/ws/metrics`)
    const wsAlerts = new WebSocket(`${wsProtocol}//${wsHost}/ws/alerts`)
    
    wsMetrics.onmessage = (e) => { 
      const d = JSON.parse(e.data); 
      if (d.type === 'METRIC_UPDATE') setMetrics({ tps: d.tps, lag: d.consumer_lag }) 
    }
    
    wsAlerts.onmessage = (e) => { 
      const p = JSON.parse(e.data); 
      if (p.type === 'NEW_ALERT') {
        setAlerts((prev) => [p.data, ...prev].slice(0, 50)) 
      }
    }
    
    return () => { wsMetrics.close(); wsAlerts.close() }
  }, [wsProtocol, wsHost])

  const handleControl = async (action: 'start' | 'stop') => {
    try {
      await fetch(`/api/v1/control/${action}`, { method: 'POST' })
      setGeneratorRunning(action === 'start')
    } catch (err) {
      console.error(err)
    }
  }

  return (
    <Box sx={{ minHeight: '100vh', bgcolor: '#f5f7fa', pb: 8 }}>
      <AppBar position="sticky" sx={{ bgcolor: 'white', color: 'text.primary', boxShadow: 1 }}>
        <Toolbar>
          <Security color="primary" sx={{ mr: 2 }} />
          <Typography variant="h6" component="div" sx={{ flexGrow: 1, fontWeight: 'bold' }}>
            Fraud Intelligence Platform
          </Typography>
          <Tabs value={activeTab} onChange={(e, val) => setActiveTab(val)}>
            <Tab icon={<Speed />} iconPosition="start" label="Control" />
            <Tab icon={<Router />} iconPosition="start" label="Kafka Stream" />
            <Tab icon={<Insights />} iconPosition="start" label="Intelligence" />
          </Tabs>
        </Toolbar>
      </AppBar>

      <Container maxWidth="lg" sx={{ mt: 6 }}>
        {/* Screen 1: Control Panel */}
        {activeTab === 0 && (
          <Card elevation={2} sx={{ maxWidth: 600, mx: 'auto', p: 2, borderRadius: 2 }}>
            <CardContent>
              <Typography variant="h5" fontWeight="bold" gutterBottom>Transaction Generator Control</Typography>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 4 }}>
                Start the simulated transaction stream to inject behavior events into Kafka.
              </Typography>
              
              <Box display="flex" gap={2} mb={4}>
                <Button 
                  variant="contained" 
                  color="success" 
                  size="large" 
                  fullWidth 
                  startIcon={<PlayArrow />}
                  disabled={generatorRunning}
                  onClick={() => handleControl('start')}
                >
                  Start Stream
                </Button>
                <Button 
                  variant="contained" 
                  color="error" 
                  size="large" 
                  fullWidth 
                  startIcon={<Stop />}
                  disabled={!generatorRunning}
                  onClick={() => handleControl('stop')}
                >
                  Stop Stream
                </Button>
              </Box>

              <Box p={2} bgcolor="#f8fafc" borderRadius={1} border={1} borderColor="#e2e8f0" display="flex" justifyContent="space-between">
                <Typography fontWeight="bold">Current Status:</Typography>
                <Chip 
                  label={generatorRunning ? "RUNNING" : "STOPPED"} 
                  color={generatorRunning ? "success" : "default"}
                  size="small"
                  sx={{ fontWeight: 'bold' }}
                />
              </Box>
            </CardContent>
          </Card>
        )}

        {/* Screen 2: Kafka Flow Animation */}
        {activeTab === 1 && (
          <Card elevation={2} sx={{ borderRadius: 2 }}>
            <CardContent>
              <Box display="flex" justifyContent="space-between" mb={6}>
                <Typography variant="h5" fontWeight="bold">Live Stream Observatory</Typography>
                <Box display="flex" gap={4}>
                  <Box textAlign="center">
                    <Typography variant="caption" color="text.secondary" fontWeight="bold">LIVE TPS</Typography>
                    <Typography variant="h4" color="primary" fontWeight="bold">{metrics.tps.toLocaleString()}</Typography>
                  </Box>
                  <Box textAlign="center">
                    <Typography variant="caption" color="text.secondary" fontWeight="bold">CONSUMER LAG</Typography>
                    <Typography variant="h4" color="success.main" fontWeight="bold">{metrics.lag}</Typography>
                  </Box>
                </Box>
              </Box>

              <Box position="relative" height={250} bgcolor="#f8fafc" borderRadius={2} border={1} borderColor="#e2e8f0" display="flex" alignItems="center" justifyContent="space-between" px={6} overflow="hidden">
                <Box zIndex={1} display="flex" flexDirection="column" alignItems="center" p={2} bgcolor="white" borderRadius={2} border={1} borderColor="#e2e8f0" boxShadow={1}>
                  <Storage sx={{ fontSize: 40, color: 'text.secondary', mb: 1 }} />
                  <Typography variant="subtitle2" fontWeight="bold">Generator</Typography>
                </Box>
                
                {generatorRunning && metrics.tps > 0 && (
                  <Box position="absolute" top={0} left={0} right={0} bottom={0} display="flex" alignItems="center" justifyContent="center">
                    <div className="w-full h-1 flex justify-between px-32 space-x-4 animate-[pulse_1s_ease-in-out_infinite]">
                      <div className="w-3 h-3 rounded-full bg-blue-500 animate-[bounce_1s_infinite]"></div>
                      <div className="w-3 h-3 rounded-full bg-blue-400 animate-[bounce_1.2s_infinite]"></div>
                      <div className="w-3 h-3 rounded-full bg-red-400 animate-[bounce_0.8s_infinite]"></div>
                      <div className="w-3 h-3 rounded-full bg-blue-500 animate-[bounce_1.5s_infinite]"></div>
                    </div>
                  </Box>
                )}

                <Box zIndex={1} display="flex" flexDirection="column" alignItems="center" p={3} bgcolor="#eff6ff" borderRadius="50%" border={4} borderColor="#bfdbfe" boxShadow={2}>
                  <Hub sx={{ fontSize: 48, color: '#1d4ed8', mb: 1 }} />
                  <Typography variant="subtitle1" fontWeight="bold" color="#1e3a8a">Apache Kafka</Typography>
                </Box>

                <Box zIndex={1} display="flex" flexDirection="column" alignItems="center" p={2} bgcolor="white" borderRadius={2} border={1} borderColor="#e2e8f0" boxShadow={1}>
                  <Speed sx={{ fontSize: 40, color: '#f97316', mb: 1 }} />
                  <Typography variant="subtitle2" fontWeight="bold">Apache Flink</Typography>
                </Box>
              </Box>
            </CardContent>
          </Card>
        )}

        {/* Screen 3: Intelligence Dashboard (Expandable) */}
        {activeTab === 2 && (
          <Card elevation={2} sx={{ borderRadius: 2 }}>
            <Box p={3} borderBottom={1} borderColor="#e2e8f0" bgcolor="#f8fafc" display="flex" justifyContent="space-between" alignItems="center">
              <Typography variant="h6" fontWeight="bold">Real-Time Intelligence</Typography>
              <Chip icon={<div className="w-2 h-2 rounded-full bg-green-500 ml-2 animate-pulse"></div>} label="Monitoring Active Stream" color="default" variant="outlined" />
            </Box>
            <CardContent sx={{ p: 0 }}>
              {alerts.length === 0 ? (
                <Box p={6} textAlign="center">
                  <Typography color="text.secondary">No transactions processed yet. Start the generator.</Typography>
                </Box>
              ) : (
                alerts.map((alert, idx) => {
                  const isHigh = alert.severity === 'HIGH RISK'
                  const isMed = alert.severity === 'MEDIUM RISK'
                  const color = isHigh ? 'error' : (isMed ? 'warning' : 'success')
                  
                  return (
                    <Accordion key={idx} disableGutters square sx={{ '&:before': { display: 'none' }, borderBottom: 1, borderColor: '#e2e8f0' }}>
                      <AccordionSummary expandIcon={<ExpandMore />}>
                        <Box display="flex" alignItems="center" justifyContent="space-between" width="100%" pr={2}>
                          <Box display="flex" alignItems="center" gap={2}>
                            <div className={`w-3 h-3 rounded-full ${isHigh ? 'bg-red-500 animate-pulse' : (isMed ? 'bg-orange-500' : 'bg-green-500')}`}></div>
                            <Typography fontWeight="bold" sx={{ width: 140 }}>{alert.transaction_id}</Typography>
                            <Typography color="text.secondary" variant="body2">{alert.reason}</Typography>
                          </Box>
                          <Box display="flex" alignItems="center" gap={3}>
                            <Chip label={alert.severity} color={color} size="small" sx={{ fontWeight: 'bold', width: 100 }} />
                            <Box textAlign="right" width={60}>
                              <Typography variant="h6" fontWeight="bold" color={`${color}.main`}>
                                {Number(alert.risk_score).toFixed(1)}
                              </Typography>
                            </Box>
                          </Box>
                        </Box>
                      </AccordionSummary>
                      <AccordionDetails sx={{ bgcolor: '#f8fafc', borderTop: 1, borderColor: '#e2e8f0' }}>
                        <Typography variant="subtitle2" fontWeight="bold" gutterBottom>Deep Analysis Metadata</Typography>
                        <Divider sx={{ mb: 2 }} />
                        <Box display="grid" gridTemplateColumns="1fr 1fr" gap={2}>
                          <Typography variant="body2"><strong>Behavior Score:</strong> {Number(alert.risk_score / 5.0).toFixed(2)}</Typography>
                          <Typography variant="body2"><strong>Final Risk Score:</strong> {Number(alert.risk_score).toFixed(1)} / 100</Typography>
                          <Typography variant="body2"><strong>Trigger Reason:</strong> {alert.reason}</Typography>
                          <Typography variant="body2"><strong>ML Engine Verdict:</strong> Anomaly Scanned</Typography>
                        </Box>
                      </AccordionDetails>
                    </Accordion>
                  )
                })
              )}
            </CardContent>
          </Card>
        )}
      </Container>
    </Box>
  )
}

export default App
