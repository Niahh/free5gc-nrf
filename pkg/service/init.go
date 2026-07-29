package service

import (
	"context"
	"io"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	nrf_context "github.com/free5gc/nrf/internal/context"
	"github.com/free5gc/nrf/internal/logger"
	"github.com/free5gc/nrf/internal/sbi"
	"github.com/free5gc/nrf/internal/sbi/consumer"
	"github.com/free5gc/nrf/internal/sbi/processor"
	"github.com/free5gc/nrf/pkg/app"
	"github.com/free5gc/nrf/pkg/factory"
	"github.com/free5gc/util/metrics"
	"github.com/free5gc/util/metrics/utils"
	"github.com/free5gc/util/mongoapi"
)

var NRF *NrfApp

var _ app.App = &NrfApp{}

type NrfApp struct {
	cfg    *factory.Config
	nrfCtx *nrf_context.NRFContext

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	sbiServer     *sbi.Server
	metricsServer *metrics.Server
	processor     *processor.Processor
	consumer      *consumer.Consumer
}

func NewApp(ctx context.Context, cfg *factory.Config, tlsKeyLogPath string) (*NrfApp, error) {
	nrf := &NrfApp{
		cfg: cfg,
		wg:  sync.WaitGroup{},
	}
	nrf.SetLogEnable(cfg.GetLogEnable())
	nrf.SetLogLevel(cfg.GetLogLevel())
	nrf.SetReportCaller(cfg.GetLogReportCaller())

	err := nrf_context.InitNrfContext()
	if err != nil {
		logger.InitLog.Errorln(err)
		return nrf, err
	}

	nrf.nrfCtx = nrf_context.GetSelf()
	nrf.ctx, nrf.cancel = context.WithCancel(ctx)

	processor, err_p := processor.NewProcessor(nrf)
	if err_p != nil {
		return nrf, err_p
	}
	nrf.processor = processor

	consumer, err_c := consumer.NewConsumer(nrf)
	if err_c != nil {
		return nrf, err_c
	}
	nrf.consumer = consumer

	if nrf.sbiServer, err = sbi.NewServer(nrf, tlsKeyLogPath); err != nil {
		return nil, err
	}

	features := map[utils.MetricTypeEnabled]bool{utils.SBI: true}
	customMetrics := make(map[utils.MetricTypeEnabled][]prometheus.Collector)
	if cfg.AreMetricsEnabled() {
		if nrf.metricsServer, err = metrics.NewServer(
			getInitMetrics(cfg, features, customMetrics), tlsKeyLogPath, logger.InitLog); err != nil {
			return nil, err
		}
	}

	NRF = nrf

	return nrf, nil
}

func getInitMetrics(
	cfg *factory.Config,
	features map[utils.MetricTypeEnabled]bool,
	customMetrics map[utils.MetricTypeEnabled][]prometheus.Collector,
) metrics.InitMetrics {
	metricsInfo := metrics.Metrics{
		BindingIPv4: cfg.GetMetricsBindingAddr(),
		Scheme:      cfg.GetMetricsScheme(),
		Namespace:   cfg.GetMetricsNamespace(),
		Port:        cfg.GetMetricsPort(),
		Tls: metrics.Tls{
			Key: cfg.GetMetricsCertKeyPath(),
			Pem: cfg.GetMetricsCertPemPath(),
		},
	}

	return metrics.NewInitMetrics(metricsInfo, "nrf", features, customMetrics)
}

func (a *NrfApp) Context() *nrf_context.NRFContext {
	return a.nrfCtx
}

func (a *NrfApp) Config() *factory.Config {
	return a.cfg
}

func (a *NrfApp) Processor() *processor.Processor {
	return a.processor
}

func (a *NrfApp) Consumer() *consumer.Consumer {
	return a.consumer
}

func (a *NrfApp) SetLogEnable(enable bool) {
	logger.MainLog.Infof("Log enable is set to [%v]", enable)
	if enable && logger.Log.Out == os.Stderr {
		return
	} else if !enable && logger.Log.Out == io.Discard {
		return
	}

	a.cfg.SetLogEnable(enable)
	if enable {
		logger.Log.SetOutput(os.Stderr)
	} else {
		logger.Log.SetOutput(io.Discard)
	}
}

func (a *NrfApp) SetLogLevel(level string) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		logger.MainLog.Warnf("Log level [%s] is invalid", level)
		return
	}

	logger.MainLog.Infof("Log level is set to [%s]", level)
	if lvl == logger.Log.GetLevel() {
		return
	}

	a.cfg.SetLogLevel(level)
	logger.Log.SetLevel(lvl)
}

func (a *NrfApp) SetReportCaller(reportCaller bool) {
	logger.MainLog.Infof("Report Caller is set to [%v]", reportCaller)
	if reportCaller == logger.Log.ReportCaller {
		return
	}

	a.cfg.SetLogReportCaller(reportCaller)
	logger.Log.SetReportCaller(reportCaller)
}

func (a *NrfApp) Start() {
	if err := mongoapi.SetMongoDB(factory.NrfConfig.Configuration.MongoDBName,
		factory.NrfConfig.Configuration.MongoDBUrl); err != nil {
		logger.InitLog.Errorf("SetMongoDB failed: %+v", err)
		return
	}

	logger.InitLog.Infoln("Server starting")

	a.wg.Add(1)
	go a.listenShutdownEvent()

	a.wg.Add(1)
	go a.sweepStaleNfProfiles()

	if err := a.sbiServer.Run(&a.wg); err != nil {
		logger.MainLog.Fatalf("Run SBI server failed: %+v", err)
	}

	if a.cfg.AreMetricsEnabled() && a.metricsServer != nil {
		go func() {
			a.metricsServer.Run(&a.wg)
		}()
	}
	a.WaitRoutineStopped()
}

func (a *NrfApp) listenShutdownEvent() {
	defer func() {
		if p := recover(); p != nil {
			// Print stack for panic to log. Fatalf() will let program exit.
			logger.MainLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
		a.wg.Done()
	}()

	<-a.ctx.Done()
	a.terminateProcedure()
}

func (a *NrfApp) Terminate() {
	a.cancel()
}

// sweepStaleNfProfiles runs the heart-beat sweeps once per heart-beat
// interval: suspend instances silent past the suspension deadline, then,
// when dropDelay is set and the startup grace has passed, deregister those
// SUSPENDED past the drop delay. The database claims each instance for a
// single replica, however many run.
func (a *NrfApp) sweepStaleNfProfiles() {
	defer a.wg.Done()

	interval := time.Duration(a.cfg.GetHeartbeatTimer()) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			a.sweepOnce(time.Since(start))
		}
	}
}

// sweepOnce recovers per tick: a panic must not silently kill the sweeper.
func (a *NrfApp) sweepOnce(uptime time.Duration) {
	defer func() {
		if p := recover(); p != nil {
			logger.MainLog.Errorf("panic in heart-beat sweep: %v\n%s", p, string(debug.Stack()))
		}
	}()
	a.processor.SuspendStaleNfProfiles(a.ctx)
	// Suspending first is safe: a fresh suspendedAt is never past the drop
	// cutoff.
	if dropGraceElapsed(uptime, a.cfg.GetHeartbeatTimer(), a.cfg.GetHeartbeatSuspendFactor()) {
		a.processor.DropStaleSuspendedNfProfiles(a.ctx)
	}
}

// dropGraceElapsed keeps the drop sweep quiet right after startup: while the
// NRF was down, live instances could not lift their suspension. One
// suspension deadline plus one interval lets them heart-beat back first.
func dropGraceElapsed(uptime time.Duration, timer, suspendFactor int) bool {
	return uptime > time.Duration(timer*(suspendFactor+1))*time.Second
}

// terminateProcedure stops serving but leaves the registry untouched: a
// terminating replica must not deregister the whole network. Dead instances
// are handled by the heart-beat sweep, here or on another replica.
func (a *NrfApp) terminateProcedure() {
	logger.MainLog.Infof("Terminating NRF...")

	a.sbiServer.Stop()

	if a.metricsServer != nil {
		a.metricsServer.Stop()
		logger.MainLog.Infof("NRF Metrics Server terminated")
	}
}

func (a *NrfApp) WaitRoutineStopped() {
	a.wg.Wait()
	logger.InitLog.Infof("NRF App terminated")
}
