package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/gin-gonic/gin"
	"github.com/mitchellh/mapstructure"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	nrf_context "github.com/free5gc/nrf/internal/context"
	"github.com/free5gc/nrf/internal/logger"
	"github.com/free5gc/nrf/internal/util"
	"github.com/free5gc/nrf/pkg/factory"
	"github.com/free5gc/openapi/models"
	"github.com/free5gc/openapi/oauth"
	timedecode "github.com/free5gc/util/mapstruct"
	"github.com/free5gc/util/mongoapi"
)

// getNFNotifyCtx returns a context carrying an NRF self-signed Bearer token
// for outbound NF status notifications.
func (p *Processor) getNFNotifyCtx(targetNF models.NrfNfManagementNfType) (context.Context, *models.ProblemDetails) {
	ctx, pd, err := nrf_context.GetSelf().GetTokenCtx("", targetNF)
	if err != nil {
		logger.NfmLog.Errorf("getNFNotifyCtx: token generation failed: %v", err)
		if pd == nil {
			pd = &models.ProblemDetails{
				Status: http.StatusInternalServerError,
				Cause:  "TOKEN_GENERATION_FAILED",
				Detail: err.Error(),
			}
		}
		return nil, pd
	}
	return ctx, nil
}

// sweepBatchSize bounds one pass of either sweep; a larger backlog drains
// over the following ticks.
const sweepBatchSize = 256

// SuspendStaleNfProfiles moves instances silent for timer * suspendFactor
// seconds to SUSPENDED, out of discovery, and notifies their subscribers
// (TS 29.510 clause 5.2.2.3).
//
// Each instance is claimed with an atomic findOneAndUpdate, so replicas
// sharing a database notify disjoint sets. The deadline uses our configured
// timer, never the stored profile's, which an NF could inflate. Instances
// without a lastHeartBeat wait for their next NFUpdate to stamp one.
func (p *Processor) SuspendStaleNfProfiles(ctx context.Context) {
	deadline := time.Duration(factory.NrfConfig.GetHeartbeatTimer()*
		factory.NrfConfig.GetHeartbeatSuspendFactor()) * time.Second
	now := time.Now().UTC()
	cutoff := now.Add(-deadline).Format(time.RFC3339)

	coll := mongoapi.Client.Database(factory.NrfConfig.Configuration.MongoDBName).
		Collection(nrf_context.NfProfileCollName)

	for i := 0; i < sweepBatchSize; i++ {
		var raw map[string]interface{}
		err := coll.FindOneAndUpdate(ctx,
			bson.M{
				"nfStatus":      string(models.NrfNfManagementNfStatus_REGISTERED),
				"lastHeartBeat": bson.M{"$lt": cutoff},
			},
			// suspendedAt starts the drop clock. The filter only matches
			// REGISTERED instances, so the stamp is fresh at every transition.
			bson.M{"$set": bson.M{
				"nfStatus":    string(models.NrfNfManagementNfStatus_SUSPENDED),
				"suspendedAt": now.Format(time.RFC3339),
			}},
			options.FindOneAndUpdate().SetReturnDocument(options.After),
		).Decode(&raw)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return
		}
		if err != nil {
			logger.NfmLog.Errorf("Suspend stale NF profiles err: %+v", err)
			return
		}

		var nfProfiles []models.NrfNfManagementNfProfile
		if err = timedecode.Decode([]map[string]interface{}{raw}, &nfProfiles); err != nil || len(nfProfiles) == 0 {
			logger.NfmLog.Errorf("Suspended NF profile decode error: %+v", err)
			continue
		}
		logger.NfmLog.Infof("NF suspended, no heart-beat received: %v [%v]",
			nfProfiles[0].NfType, nfProfiles[0].NfInstanceId)
		p.notifySubscribers(models.NotificationEventType_PROFILE_CHANGED, &nfProfiles[0])
	}
}

// notifySubscribers notifies every subscriber of the profile; failures are
// logged so one unreachable target does not starve the rest. DEREGISTERED
// carries no payload, as in NFDeregisterProcedure.
func (p *Processor) notifySubscribers(
	event models.NotificationEventType,
	nfProfile *models.NrfNfManagementNfProfile,
) {
	payload := nfProfile
	if event == models.NotificationEventType_DEREGISTERED {
		payload = nil
	}
	nfInstanceUri := nrf_context.GetNfInstanceURI(nfProfile.NfInstanceId)
	for _, target := range nrf_context.GetNofificationUri(nfProfile) {
		notifCtx, pd := p.getNFNotifyCtx(target.TargetNf)
		if pd != nil {
			logger.NfmLog.Errorf("Notify %s failed: %+v", target.Uri, pd)
			continue
		}
		if pd = p.Consumer().SendNFStatusNotify(notifCtx,
			event, nfInstanceUri, target.Uri, payload); pd != nil {
			logger.NfmLog.Errorf("Notify %s failed: %+v", target.Uri, pd)
		}
	}
}

// DropStaleSuspendedNfProfiles deregisters instances that stayed SUSPENDED
// for more than dropDelay seconds (TS 29.510 clause 5.2.2.3), and does
// nothing when dropDelay is unset. Without it, every NF restarting under a
// fresh nfInstanceId leaves a stale SUSPENDED document behind forever.
//
// Guards against dropping a live instance:
//   - The delay counts from suspendedAt, so after an NRF outage an instance
//     still gets a full heart-beat window (the caller's startup grace).
//   - An instance with a fresh lastHeartBeat is never claimed.
//   - findOneAndDelete claims atomically: with several replicas, exactly one
//     deregisters an instance, and its subscribers hear DEREGISTERED once.
//   - An NF re-registers on a heart-beat 404, so an instance dropped while
//     still alive is back in the registry on its next heart-beat.
func (p *Processor) DropStaleSuspendedNfProfiles(ctx context.Context) {
	// A zero delay would put the cutoff at now and drop every suspended
	// instance on sight, so the guard stays here rather than in the caller.
	delay := factory.NrfConfig.GetHeartbeatDropDelay()
	if delay <= 0 {
		return
	}
	now := time.Now().UTC()
	cutoff := now.Add(-time.Duration(delay) * time.Second).Format(time.RFC3339)

	coll := mongoapi.Client.Database(factory.NrfConfig.Configuration.MongoDBName).
		Collection(nrf_context.NfProfileCollName)

	// SUSPENDED documents without suspendedAt (older builds, or an NF that
	// patched itself SUSPENDED) start their clock now: dropped one full delay
	// later, never on sight.
	if _, err := coll.UpdateMany(ctx,
		bson.M{
			"nfStatus":    string(models.NrfNfManagementNfStatus_SUSPENDED),
			"suspendedAt": bson.M{"$exists": false},
		},
		bson.M{"$set": bson.M{"suspendedAt": now.Format(time.RFC3339)}}); err != nil {
		logger.NfmLog.Errorf("Backfill suspendedAt err: %+v", err)
	}

	for i := 0; i < sweepBatchSize; i++ {
		var raw map[string]interface{}
		err := coll.FindOneAndDelete(ctx, bson.M{
			"nfStatus":    string(models.NrfNfManagementNfStatus_SUSPENDED),
			"suspendedAt": bson.M{"$lt": cutoff},
			"$or": []bson.M{
				{"lastHeartBeat": bson.M{"$lt": cutoff}},
				{"lastHeartBeat": bson.M{"$exists": false}},
			},
		}).Decode(&raw)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return
		}
		if err != nil {
			logger.NfmLog.Errorf("Drop stale suspended NF profiles err: %+v", err)
			return
		}

		var nfProfiles []models.NrfNfManagementNfProfile
		if err = timedecode.Decode([]map[string]interface{}{raw}, &nfProfiles); err != nil || len(nfProfiles) == 0 {
			logger.NfmLog.Errorf("Dropped NF profile decode error: %+v", err)
			continue
		}
		profile := &nfProfiles[0]
		logger.NfmLog.Infof("NF profile dropped, SUSPENDED since %v: %v [%v]",
			raw["suspendedAt"], profile.NfType, profile.NfInstanceId)

		// Same cleanup as NFDeregisterProcedure, logged rather than propagated:
		// no client waits on the sweep.
		p.notifySubscribers(models.NotificationEventType_DEREGISTERED, profile)
		putData := bson.M{
			"_link.item": bson.M{"href": nrf_context.GetNfInstanceURI(profile.NfInstanceId)},
			"multi":      true,
		}
		if pullErr := mongoapi.RestfulAPIPullOne("urilist", bson.M{"nfType": profile.NfType}, putData); pullErr != nil {
			logger.NfmLog.Errorf("Drop urilist cleanup err: %+v", pullErr)
		}
		if factory.NrfConfig.GetOAuth() {
			nfCertPath := oauth.GetNFCertPath(
				factory.NrfConfig.GetCertBasePath(), string(profile.NfType), profile.NfInstanceId)
			if removeErr := os.Remove(nfCertPath); removeErr != nil {
				logger.NfmLog.Warningf("Can not delete NFCertPem file: %v: %v", nfCertPath, removeErr)
			}
		}
	}
}

// touchLastHeartBeat records the contact the sweeps measure against. Stored
// as fixed-width UTC RFC3339: $lt needs lexicographic order to match
// chronological order, which fractional seconds or a zone offset would break.
func touchLastHeartBeat(nfInstanceID string) error {
	_, err := mongoapi.Client.Database(factory.NrfConfig.Configuration.MongoDBName).
		Collection(nrf_context.NfProfileCollName).
		UpdateOne(context.Background(),
			bson.M{"nfInstanceId": nfInstanceID},
			bson.M{"$set": bson.M{"lastHeartBeat": time.Now().UTC().Format(time.RFC3339)}})
	return err
}

// clearSuspension moves a suspended instance back to REGISTERED. The status
// filter makes racing the sweep safe in both orders.
func clearSuspension(nfInstanceID string) error {
	_, err := mongoapi.Client.Database(factory.NrfConfig.Configuration.MongoDBName).
		Collection(nrf_context.NfProfileCollName).
		UpdateOne(context.Background(),
			bson.M{
				"nfInstanceId": nfInstanceID,
				"nfStatus":     string(models.NrfNfManagementNfStatus_SUSPENDED),
			},
			bson.M{"$set": bson.M{"nfStatus": string(models.NrfNfManagementNfStatus_REGISTERED)}})
	return err
}

func (p *Processor) HandleNFDeregisterRequest(c *gin.Context, nfInstanceId string) {
	logger.NfmLog.Infoln("Handle NFDeregisterRequest")

	problemDetails := p.NFDeregisterProcedure(nfInstanceId)

	if problemDetails != nil {
		util.GinProblemJson(c, problemDetails)
	} else {
		c.Status(http.StatusNoContent)
	}
}

func (p *Processor) HandleGetNFInstanceRequest(c *gin.Context, nfInstanceId string) {
	logger.NfmLog.Infoln("Handle GetNFInstanceRequest")

	p.GetNFInstanceProcedure(c, nfInstanceId)
}

func (p *Processor) HandleNFRegisterRequest(
	c *gin.Context,
	nfProfile *models.NrfNfManagementNfProfile,
	rawProfile []byte,
) {
	logger.NfmLog.Infoln("Handle NFRegisterRequest")

	p.NFRegisterProcedure(c, nfProfile, rawProfile)
}

func (p *Processor) HandleUpdateNFInstanceRequest(c *gin.Context, patchJSON []byte, nfInstanceID string) {
	logger.NfmLog.Infoln("Handle UpdateNFInstanceRequest")

	response, problemDetails := p.UpdateNFInstanceProcedure(nfInstanceID, patchJSON)
	if problemDetails != nil {
		util.GinProblemJson(c, problemDetails)
		return
	}
	if response == nil {
		c.Status(http.StatusNoContent)
		return
	}
	c.JSON(http.StatusOK, response)
}

func (p *Processor) HandleGetNFInstancesRequest(c *gin.Context, nfType string, limit int) {
	logger.NfmLog.Infoln("Handle GetNFInstancesRequest")

	response, problemDetails := p.GetNFInstancesProcedure(nfType, limit)
	if response != nil {
		logger.NfmLog.Traceln("GetNFInstances success")
		c.JSON(http.StatusOK, response)
		return
	} else if problemDetails != nil {
		logger.NfmLog.Traceln("GetNFInstances failed")
		util.GinProblemJson(c, problemDetails)
		return
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	logger.NfmLog.Traceln("GetNFInstances failed")
	util.GinProblemJson(c, problemDetails)
}

func (p *Processor) HandleRemoveSubscriptionRequest(c *gin.Context, subscriptionID string) {
	logger.NfmLog.Infoln("Handle RemoveSubscription")

	p.RemoveSubscriptionProcedure(subscriptionID)

	c.Status(http.StatusNoContent)
}

func (p *Processor) HandleUpdateSubscriptionRequest(
	c *gin.Context,
	subscriptionID string,
	patchJSON []byte,
) {
	logger.NfmLog.Infoln("Handle UpdateSubscription")

	response := p.UpdateSubscriptionProcedure(subscriptionID, patchJSON)
	if response == nil {
		c.Status(http.StatusNoContent)
		return
	}
	c.JSON(http.StatusOK, response)
}

func (p *Processor) HandleCreateSubscriptionRequest(
	c *gin.Context,
	subscription models.NrfNfManagementSubscriptionData,
) {
	logger.NfmLog.Infoln("Handle CreateSubscriptionRequest")

	response, problemDetails := p.CreateSubscriptionProcedure(subscription)
	if response != nil {
		logger.NfmLog.Traceln("CreateSubscription success")
		c.JSON(http.StatusCreated, response)
		return
	} else if problemDetails != nil {
		logger.NfmLog.Traceln("CreateSubscription failed")
		util.GinProblemJson(c, problemDetails)
		return
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	logger.NfmLog.Traceln("CreateSubscription failed")
	util.GinProblemJson(c, problemDetails)
}

func (p *Processor) CreateSubscriptionProcedure(
	subscription models.NrfNfManagementSubscriptionData,
) (bson.M, *models.ProblemDetails) {
	subscriptionID, err := nrf_context.SetsubscriptionId()
	if err != nil {
		logger.NfmLog.Errorf("Unable to create subscription ID in CreateSubscriptionProcedure: %+v", err)
		return nil, &models.ProblemDetails{
			Status: http.StatusInternalServerError,
			Cause:  "CREATE_SUBSCRIPTION_ERROR",
		}
	}
	subscription.SubscriptionId = subscriptionID

	tmp, err := json.Marshal(subscription)
	if err != nil {
		logger.NfmLog.Errorln("Marshal error in CreateSubscriptionProcedure: ", err)
		return nil, &models.ProblemDetails{
			Status: http.StatusInternalServerError,
			Cause:  "CREATE_SUBSCRIPTION_ERROR",
		}
	}
	putData := bson.M{}
	err = json.Unmarshal(tmp, &putData)
	if err != nil {
		logger.NfmLog.Errorln("Unmarshal error in CreateSubscriptionProcedure: ", err)
		return nil, &models.ProblemDetails{
			Status: http.StatusInternalServerError,
			Cause:  "CREATE_SUBSCRIPTION_ERROR",
		}
	}

	// TODO: need to store Condition !
	existed, err := mongoapi.RestfulAPIPost("Subscriptions", bson.M{"subscriptionId": subscription.SubscriptionId},
		putData) // subscription id not exist before
	if err != nil || existed {
		if err != nil {
			logger.NfmLog.Errorf("CreateSubscriptionProcedure err: %+v", err)
		}
		problemDetails := &models.ProblemDetails{
			Status: http.StatusInternalServerError,
			Cause:  "CREATE_SUBSCRIPTION_ERROR",
		}
		return nil, problemDetails
	}
	return putData, nil
}

func (p *Processor) UpdateSubscriptionProcedure(subscriptionID string, patchJSON []byte) map[string]interface{} {
	collName := "Subscriptions"
	filter := bson.M{"subscriptionId": subscriptionID}

	if err := mongoapi.RestfulAPIJSONPatch(collName, filter, patchJSON); err != nil {
		return nil
	} else {
		if response, err1 := mongoapi.RestfulAPIGetOne(collName, filter); err1 == nil {
			return response
		}
		return nil
	}
}

func (p *Processor) RemoveSubscriptionProcedure(subscriptionID string) {
	collName := "Subscriptions"
	filter := bson.M{"subscriptionId": subscriptionID}

	if err := mongoapi.RestfulAPIDeleteMany(collName, filter); err != nil {
		logger.NfmLog.Errorf("RemoveSubscriptionProcedure err: %+v", err)
	}
}

func (p *Processor) GetNFInstancesProcedure(nfType string, limit int) (*nrf_context.UriList, *models.ProblemDetails) {
	collName := "urilist"
	filter := bson.M{"nfType": nfType}
	if nfType == "" {
		// if the query parameter is not present, do not filter by nfType
		filter = bson.M{}
	}

	ULs, err := mongoapi.RestfulAPIGetMany(collName, filter)
	if err != nil {
		logger.NfmLog.Errorf("GetNFInstancesProcedure err: %+v", err)
		problemDetail := &models.ProblemDetails{
			Title:  "System failure",
			Status: http.StatusInternalServerError,
			Detail: err.Error(),
			Cause:  "SYSTEM_FAILURE",
		}
		return nil, problemDetail
	}
	logger.NfmLog.Infoln("ULs: ", ULs)
	rspUriList := &nrf_context.UriList{}
	for _, UL := range ULs {
		originalUL := &nrf_context.UriList{}
		if err = mapstructure.Decode(UL, originalUL); err != nil {
			logger.NfmLog.Errorf("Decode error in GetNFInstancesProcedure: %+v", err)
			problemDetail := &models.ProblemDetails{
				Title:  "System failure",
				Status: http.StatusInternalServerError,
				Detail: err.Error(),
				Cause:  "SYSTEM_FAILURE",
			}
			return nil, problemDetail
		}
		rspUriList.Link.Item = append(rspUriList.Link.Item, originalUL.Link.Item...)
		if nfType != "" && rspUriList.NfType == "" {
			rspUriList.NfType = originalUL.NfType
		}
	}

	nrf_context.NnrfUriListLimit(rspUriList, limit)
	return rspUriList, nil
}

func (p *Processor) NFDeregisterProcedure(nfInstanceID string) *models.ProblemDetails {
	collName := nrf_context.NfProfileCollName
	filter := bson.M{"nfInstanceId": nfInstanceID}

	nfProfilesRaw, err := mongoapi.RestfulAPIGetMany(collName, filter)
	if err != nil {
		logger.NfmLog.Errorf("NFDeregisterProcedure err: %+v", err)
		problemDetail := &models.ProblemDetails{
			Title:  "System failure",
			Status: http.StatusInternalServerError,
			Detail: err.Error(),
			Cause:  "SYSTEM_FAILURE",
		}
		return problemDetail
	}
	const dbWaitTime = time.Duration(500) * time.Millisecond
	time.Sleep(dbWaitTime)

	if err = mongoapi.RestfulAPIDeleteMany(collName, filter); err != nil {
		logger.NfmLog.Errorf("NFDeregisterProcedure err: %+v", err)
		problemDetail := &models.ProblemDetails{
			Title:  "System failure",
			Status: http.StatusInternalServerError,
			Detail: err.Error(),
			Cause:  "SYSTEM_FAILURE",
		}
		return problemDetail
	}

	// nfProfile data for response
	var nfProfiles []models.NrfNfManagementNfProfile
	if err = timedecode.Decode(nfProfilesRaw, &nfProfiles); err != nil {
		logger.NfmLog.Warnln("Time decode error: ", err)
		problemDetails := &models.ProblemDetails{
			Status: http.StatusInternalServerError,
			Cause:  "NOTIFICATION_ERROR",
			Detail: err.Error(),
		}
		return problemDetails
	}

	if len(nfProfiles) == 0 {
		logger.NfmLog.Warnf("NFProfile[%s] not found", nfInstanceID)
		problemDetails := &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "RESOURCE_URI_STRUCTURE_NOT_FOUND",
			Detail: fmt.Sprintf("NFProfile[%s] not found", nfInstanceID),
		}
		return problemDetails
	}

	uriList := nrf_context.GetNofificationUri(&nfProfiles[0])
	nfInstanceType := nfProfiles[0].NfType
	nfInstanceUri := nrf_context.GetNfInstanceURI(nfInstanceID)
	// set info for NotificationData
	Notification_event := models.NotificationEventType_DEREGISTERED

	for _, target := range uriList {
		notifCtx, pd := p.getNFNotifyCtx(target.TargetNf)
		if pd != nil {
			return pd
		}
		problemDetails := p.Consumer().SendNFStatusNotify(notifCtx, Notification_event, nfInstanceUri, target.Uri, nil)
		if problemDetails != nil {
			return problemDetails
		}
	}

	collNameURI := "urilist"
	filterURI := bson.M{"nfType": nfProfiles[0].NfType}
	putData := bson.M{"_link.item": bson.M{"href": nfInstanceUri}, "multi": true}
	if err = mongoapi.RestfulAPIPullOne(collNameURI, filterURI, putData); err != nil {
		logger.NfmLog.Errorf("NFDeregisterProcedure err: %+v", err)
		problemDetail := &models.ProblemDetails{
			Title:  "System failure",
			Status: http.StatusInternalServerError,
			Detail: err.Error(),
			Cause:  "SYSTEM_FAILURE",
		}
		return problemDetail
	}
	if factory.NrfConfig.GetOAuth() {
		nfCertPath := oauth.GetNFCertPath(factory.NrfConfig.GetCertBasePath(), string(nfInstanceType), nfInstanceID)
		if removeErr := os.Remove(nfCertPath); removeErr != nil {
			logger.NfmLog.Warningf("Can not delete NFCertPem file: %v: %v", nfCertPath, removeErr)
		}
	}
	logger.NfmLog.Infof("NfDeregister Success: %v [%v]", nfInstanceType, nfInstanceID)
	return nil
}

func (p *Processor) UpdateNFInstanceProcedure(
	nfInstanceID string,
	patchJSON []byte,
) (map[string]interface{}, *models.ProblemDetails) {
	if err := validateNfProfilePatch(patchJSON); err != nil {
		logger.NfmLog.Warnf("Reject invalid NF profile patch: %v", err)
		return nil, &models.ProblemDetails{
			Title:  "Malformed request syntax",
			Status: http.StatusBadRequest,
			Detail: err.Error(),
		}
	}

	collName := nrf_context.NfProfileCollName
	filter := bson.M{"nfInstanceId": nfInstanceID}

	// read the original NF profile from MongoDB
	nf, err := mongoapi.RestfulAPIGetOne(collName, filter)
	if err != nil {
		logger.NfmLog.Errorf("UpdateNFInstanceProcedure err: %+v", err)
		return nil, &models.ProblemDetails{
			Title:  "System failure",
			Status: http.StatusInternalServerError,
			Detail: err.Error(),
			Cause:  "SYSTEM_FAILURE",
		}
	}
	if nf == nil {
		logger.NfmLog.Warnf("NFProfile[%s] not found", nfInstanceID)
		return nil, &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "RESOURCE_URI_STRUCTURE_NOT_FOUND",
			Detail: fmt.Sprintf("NFProfile[%s] not found", nfInstanceID),
		}
	}

	// apply the JSON Patch to the original NF profile
	currentJSON, err := json.Marshal(nf)
	if err != nil {
		logger.NfmLog.Errorf("UpdateNFInstanceProcedure err: %+v", err)
		return nil, &models.ProblemDetails{
			Title:  "System failure",
			Status: http.StatusInternalServerError,
			Detail: err.Error(),
			Cause:  "SYSTEM_FAILURE",
		}
	}
	patch, err := jsonpatch.DecodePatch(patchJSON)
	if err != nil {
		return nil, &models.ProblemDetails{
			Title:  "Malformed request syntax",
			Status: http.StatusBadRequest,
			Detail: err.Error(),
		}
	}
	patchedJSON, err := patch.Apply(currentJSON)
	if err != nil {
		return nil, &models.ProblemDetails{
			Title:  "Malformed request syntax",
			Status: http.StatusBadRequest,
			Detail: err.Error(),
		}
	}

	// validate the patched NF profile
	var patchedProfile models.NrfNfManagementNfProfile
	if err = json.Unmarshal(patchedJSON, &patchedProfile); err != nil {
		return nil, &models.ProblemDetails{
			Title:  "Malformed request syntax",
			Status: http.StatusBadRequest,
			Detail: err.Error(),
		}
	}
	if err := validateNfProfileJSON(patchedJSON, &patchedProfile); err != nil {
		logger.NfmLog.Warnf("Reject invalid NF profile patch result: %v", err)
		return nil, &models.ProblemDetails{
			Title:  "Malformed request syntax",
			Status: http.StatusBadRequest,
			Detail: err.Error(),
		}
	}
	var originalProfile models.NrfNfManagementNfProfile
	if err = json.Unmarshal(currentJSON, &originalProfile); err != nil {
		logger.NfmLog.Errorf("UpdateNFInstanceProcedure err: %+v", err)
		return nil, &models.ProblemDetails{
			Title:  "System failure",
			Status: http.StatusInternalServerError,
			Detail: err.Error(),
			Cause:  "SYSTEM_FAILURE",
		}
	}
	if err := checkPatchInvariants(&originalProfile, &patchedProfile); err != nil {
		logger.NfmLog.Warnf("Reject invalid NF profile patch result: %v", err)
		return nil, &models.ProblemDetails{
			Title:  "Malformed request syntax",
			Status: http.StatusBadRequest,
			Detail: err.Error(),
		}
	}

	// The NFUpdate is the heart-beat (TS 29.510 clause 5.2.2.3): stamp it
	// before the patch stores REGISTERED, or a concurrent sweep could claim
	// the instance on its stale timestamp and re-suspend it. Failures are
	// logged; the next heart-beat retries.
	if err = touchLastHeartBeat(nfInstanceID); err != nil {
		logger.NfmLog.Errorf("UpdateNFInstanceProcedure record heart-beat err: %+v", err)
	}

	if err := mongoapi.RestfulAPIJSONPatch(collName, filter, patchJSON); err != nil {
		logger.NfmLog.Errorf("UpdateNFInstanceProcedure err: %+v", err)
		return nil, &models.ProblemDetails{
			Title:  "Malformed request syntax",
			Status: http.StatusBadRequest,
			Detail: err.Error(),
		}
	}
	// A load-only heart-beat is still a heart-beat (clause 5.2.2.3): an
	// NFUpdate that does not write nfStatus itself lifts a suspension, or a
	// suspended instance that keeps heart-beating would stay SUSPENDED forever.
	if !nfStatusPatched(patchJSON) {
		if err = clearSuspension(nfInstanceID); err != nil {
			logger.NfmLog.Errorf("UpdateNFInstanceProcedure clear suspension err: %+v", err)
		}
	}

	nf, err = mongoapi.RestfulAPIGetOne(collName, filter)
	if err != nil {
		logger.NfmLog.Errorf("UpdateNFInstanceProcedure err: %+v", err)
		return nil, &models.ProblemDetails{
			Title:  "System failure",
			Status: http.StatusInternalServerError,
			Detail: err.Error(),
			Cause:  "SYSTEM_FAILURE",
		}
	}
	// The instance can be deregistered between the writes above and this read.
	if nf == nil {
		logger.NfmLog.Warnf("NFProfile[%s] not found", nfInstanceID)
		return nil, &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "RESOURCE_URI_STRUCTURE_NOT_FOUND",
			Detail: fmt.Sprintf("NFProfile[%s] not found", nfInstanceID),
		}
	}

	// Sweep bookkeeping, not part of the exposed NF profile.
	delete(nf, "lastHeartBeat")
	delete(nf, "suspendedAt")

	nfProfilesRaw := []map[string]interface{}{
		nf,
	}

	var nfProfiles []models.NrfNfManagementNfProfile
	if err = timedecode.Decode(nfProfilesRaw, &nfProfiles); err != nil {
		logger.NfmLog.Errorf("UpdateNFInstanceProcedure err: %+v", err)
		return nil, &models.ProblemDetails{
			Title:  "System failure",
			Status: http.StatusInternalServerError,
			Detail: err.Error(),
			Cause:  "SYSTEM_FAILURE",
		}
	}

	if len(nfProfiles) == 0 {
		logger.NfmLog.Warnf("NFProfile[%s] not found", nfInstanceID)
		return nil, &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "RESOURCE_URI_STRUCTURE_NOT_FOUND",
			Detail: fmt.Sprintf("NFProfile[%s] not found", nfInstanceID),
		}
	}

	uriList := nrf_context.GetNofificationUri(&nfProfiles[0])

	// set info for NotificationData
	Notification_event := models.NotificationEventType_PROFILE_CHANGED
	nfInstanceUri := nrf_context.GetNfInstanceURI(nfInstanceID)

	for _, target := range uriList {
		notifCtx, pd := p.getNFNotifyCtx(target.TargetNf)
		if pd != nil {
			logger.NfmLog.Errorf("UpdateNFInstanceProcedure SendNotification Error: %+v", pd)
			continue
		}
		p.Consumer().SendNFStatusNotify(notifCtx, Notification_event, nfInstanceUri, target.Uri, &nfProfiles[0])
	}
	return nf, nil
}

func validateNfProfilePatch(patchJSON []byte) error {
	var operations []map[string]interface{}
	if err := json.Unmarshal(patchJSON, &operations); err != nil {
		return fmt.Errorf("invalid JSON Patch payload")
	}

	for _, operation := range operations {
		path, ok := operation["path"].(string)
		if !ok {
			continue
		}

		normalizedPath := strings.ToLower(strings.TrimSpace(path))
		if normalizedPath == "/nfinstanceid" || strings.HasPrefix(normalizedPath, "/nfinstanceid/") {
			return fmt.Errorf("nfInstanceId is immutable and cannot be modified")
		}
		// heartBeatTimer is ours to set (clause 5.2.2.3): NFs reset their ticker
		// from the response, so patching it to 0 would self-suspend.
		if normalizedPath == "/heartbeattimer" || strings.HasPrefix(normalizedPath, "/heartbeattimer/") {
			return fmt.Errorf("heartBeatTimer is set by the NRF and cannot be modified")
		}

		from, ok := operation["from"].(string)
		if !ok {
			continue
		}

		normalizedFrom := strings.ToLower(strings.TrimSpace(from))
		if normalizedFrom == "/nfinstanceid" || strings.HasPrefix(normalizedFrom, "/nfinstanceid/") {
			return fmt.Errorf("nfInstanceId is immutable and cannot be modified")
		}
		if normalizedFrom == "/heartbeattimer" || strings.HasPrefix(normalizedFrom, "/heartbeattimer/") {
			return fmt.Errorf("heartBeatTimer is set by the NRF and cannot be modified")
		}
	}

	return nil
}

// checkPatchInvariants rejects a patch whose result changed an NRF-owned
// field. The path guards in validateNfProfilePatch cannot see a whole-document
// op (path "", RFC 6901), so the applied result is checked too.
func checkPatchInvariants(original, patched *models.NrfNfManagementNfProfile) error {
	if patched.NfInstanceId != original.NfInstanceId {
		return fmt.Errorf("nfInstanceId is immutable and cannot be modified")
	}
	if patched.HeartBeatTimer != original.HeartBeatTimer {
		return fmt.Errorf("heartBeatTimer is set by the NRF and cannot be modified")
	}
	return nil
}

// nfStatusPatched reports whether the patch writes nfStatus itself: then the
// status is the NF's explicit choice; otherwise the update is a pure
// heart-beat and may clear a suspension. The empty path is the whole-document
// pointer.
//
// The comparison is byte-exact, JSON Pointers are case-sensitive: a patch
// naming /NfStatus writes a separate key and must still count as a plain
// heart-beat. Unlike validateNfProfilePatch, normalizing here would suppress
// a correction instead of merely rejecting more patches.
func nfStatusPatched(patchJSON []byte) bool {
	var operations []struct {
		Op   string `json:"op"`
		Path string `json:"path"`
	}
	if err := json.Unmarshal(patchJSON, &operations); err != nil {
		return false
	}
	for _, operation := range operations {
		if operation.Op == "test" {
			// test asserts and never writes (RFC 6902).
			continue
		}
		switch operation.Path {
		case "", "/nfStatus":
			return true
		}
	}
	return false
}

func (p *Processor) GetNFInstanceProcedure(c *gin.Context, nfInstanceID string) {
	collName := nrf_context.NfProfileCollName
	filter := bson.M{"nfInstanceId": nfInstanceID}
	response, err := mongoapi.RestfulAPIGetOne(collName, filter)
	if err != nil {
		logger.NfmLog.Errorf("GetNFInstanceProcedure err: %+v", err)
		return
	}

	if response == nil {
		problemDetails := &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "Mongoapi not found",
		}
		util.GinProblemJson(c, problemDetails)
		return
	}
	// Sweep bookkeeping, not part of the exposed NF profile.
	delete(response, "lastHeartBeat")
	delete(response, "suspendedAt")
	c.JSON(http.StatusOK, response)
}

func (p *Processor) NFRegisterProcedure(
	c *gin.Context,
	nfProfile *models.NrfNfManagementNfProfile,
	rawProfile []byte,
) {
	logger.NfmLog.Traceln("[NRF] In NFRegisterProcedure")

	if err := validateNfProfileJSON(rawProfile, nfProfile); err != nil {
		problemDetails := &models.ProblemDetails{
			Title:  "Malformed request syntax",
			Status: http.StatusBadRequest,
			Detail: err.Error(),
		}
		util.GinProblemJson(c, problemDetails)
		return
	}

	if nfProfile.NfInstanceId == "" || nfProfile.NfType == "" || nfProfile.NfStatus == "" {
		problemDetails := &models.ProblemDetails{
			Title:  "Mandatory IE missing",
			Status: http.StatusBadRequest,
			Detail: "nfInstanceId, nfType and nfStatus are required",
			Cause:  "MANDATORY_IE_MISSING",
		}
		util.GinProblemJson(c, problemDetails)
		return
	}

	var nf models.NrfNfManagementNfProfile

	err := nrf_context.NnrfNFManagementDataModel(&nf, nfProfile)
	if err != nil {
		problemDetails := &models.ProblemDetails{
			Title:  "Malformed request syntax",
			Status: http.StatusBadRequest,
			Detail: err.Error(),
		}
		util.GinProblemJson(c, problemDetails)
		return
	}

	if err := validateNfProfile(&nf); err != nil {
		problemDetails := &models.ProblemDetails{
			Title:  "Malformed request syntax",
			Status: http.StatusBadRequest,
			Detail: err.Error(),
		}
		util.GinProblemJson(c, problemDetails)
		return
	}

	// make location header
	locationHeaderValue := nrf_context.SetLocationHeader(nfProfile)
	// Marshal nf to bson
	tmp, err := json.Marshal(nf)
	if err != nil {
		logger.NfmLog.Errorln("Marshal error in NFRegisterProcedure: ", err)
		problemDetails := &models.ProblemDetails{
			Title:  "System failure",
			Status: http.StatusInternalServerError,
			Detail: err.Error(),
			Cause:  "SYSTEM_FAILURE",
		}
		util.GinProblemJson(c, problemDetails)
		return
	}
	putData := bson.M{}
	err = json.Unmarshal(tmp, &putData)
	if err != nil {
		logger.NfmLog.Errorln("Unmarshal error in NFRegisterProcedure: ", err)
		problemDetails := &models.ProblemDetails{
			Title:  "System failure",
			Status: http.StatusInternalServerError,
			Detail: err.Error(),
			Cause:  "SYSTEM_FAILURE",
		}
		util.GinProblemJson(c, problemDetails)
		return
	}
	// set db info
	collName := nrf_context.NfProfileCollName
	nfInstanceId := nf.NfInstanceId
	filter := bson.M{"nfInstanceId": nfInstanceId}

	// Update NF Profile case
	existed, err := mongoapi.RestfulAPIPutOne(collName, filter, putData)
	if err != nil {
		logger.NfmLog.Errorf("NFRegisterProcedure err: %+v", err)
		problemDetails := &models.ProblemDetails{
			Title:  "System failure",
			Status: http.StatusInternalServerError,
			Detail: err.Error(),
			Cause:  "SYSTEM_FAILURE",
		}
		util.GinProblemJson(c, problemDetails)
		return
	}

	// The heart-beat window opens at registration. Written beside the profile,
	// not into putData, so it never leaks into the response bodies below.
	// Failures are logged; the first heart-beat stamps the field anyway.
	if err = touchLastHeartBeat(nfInstanceId); err != nil {
		logger.NfmLog.Errorf("NFRegisterProcedure record heart-beat err: %+v", err)
	}

	if existed {
		logger.NfmLog.Infoln("NFRegister NfProfile Update:", nfInstanceId)
		uriList := nrf_context.GetNofificationUri(&nf)

		// set info for NotificationData
		Notification_event := models.NotificationEventType_PROFILE_CHANGED
		nfInstanceUri := locationHeaderValue

		// receive the rsp from handler
		for _, target := range uriList {
			notifCtxUpdate, pd := p.getNFNotifyCtx(target.TargetNf)
			if pd != nil {
				util.GinProblemJson(c, pd)
				return
			}
			problemDetails := p.Consumer().SendNFStatusNotify(notifCtxUpdate,
				Notification_event, nfInstanceUri, target.Uri, nfProfile)
			if problemDetails != nil {
				util.GinProblemJson(c, problemDetails)
				return
			}
		}

		c.Writer.Header().Add("Location", locationHeaderValue)
		c.JSON(http.StatusOK, putData)
		return
	} else { // Create NF Profile case
		logger.NfmLog.Infoln("Create NF Profile:", nfInstanceId)
		uriList := nrf_context.GetNofificationUri(&nf)
		// set info for NotificationData
		Notification_event := models.NotificationEventType_REGISTERED
		nfInstanceUri := locationHeaderValue

		for _, target := range uriList {
			notifCtxCreate, pd := p.getNFNotifyCtx(target.TargetNf)
			if pd != nil {
				util.GinProblemJson(c, pd)
				return
			}
			problemDetails := p.Consumer().SendNFStatusNotify(notifCtxCreate,
				Notification_event, nfInstanceUri, target.Uri, nfProfile)
			if problemDetails != nil {
				util.GinProblemJson(c, problemDetails)
				return
			}
		}
		c.Writer.Header().Add("Location", locationHeaderValue)

		if factory.NrfConfig.GetOAuth() {
			// Generate NF's pubkey certificate with root certificate
			err = nrf_context.SignNFCert(string(nf.NfType), nfInstanceId)
			if err != nil {
				logger.NfmLog.Warnln(err)
			}
		}
		c.JSON(http.StatusCreated, putData)
		return
	}
}
