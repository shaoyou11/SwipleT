//go:build darwin && cgo

#import "SWFrameDecoder.h"

#import <VideoToolbox/VideoToolbox.h>
#import <stdatomic.h>

#import "SWAccessUnit.h"
#import "SWError.h"
#import "SWGoBridge.h"
#import "SWHEVC.h"

@implementation SWFrameDecoder {
    CMVideoFormatDescriptionRef _format;
    VTDecompressionSessionRef _session;
    BOOL _waitingForKeyframe;
    atomic_bool _failed;
    atomic_bool _reportedDecodeError;
    atomic_int _failedStatus;
    OSStatus _reportedStatus;
    SWRTPClock *_clock;
}

- (instancetype)init {
    self = [super init];
    if (self) {
        _waitingForKeyframe = YES;
        _clock = [SWRTPClock new];
    }
    return self;
}

- (void)dealloc {
    [self closeSession];
    if (_format != NULL) {
        CFRelease(_format);
    }
}

- (void)closeSession {
    if (_session != NULL) {
        VTDecompressionSessionInvalidate(_session);
        CFRelease(_session);
        _session = NULL;
    }
}

- (void)invalidate {
    [self closeSession];
    _waitingForKeyframe = YES;
}

- (BOOL)makeSession:(CMVideoFormatDescriptionRef)format {
    [self closeSession];
    NSDictionary *attributes = @{
        (__bridge id)kCVPixelBufferPixelFormatTypeKey: @(kCVPixelFormatType_32BGRA),
        (__bridge id)kCVPixelBufferIOSurfacePropertiesKey: @{},
    };
    VTDecompressionSessionRef created = NULL;
    OSStatus status = VTDecompressionSessionCreate(NULL, format, NULL, (__bridge CFDictionaryRef)attributes, NULL, &created);
    if (status != noErr || created == NULL) {
        SWLogError([NSString stringWithFormat:@"create the screen decoder: %@", SWErrorMessageOf(SWErrorStatus(status))]);
        return NO;
    }
    VTSessionSetProperty(created, kVTDecompressionPropertyKey_RealTime, kCFBooleanTrue);
    _session = created;
    return YES;
}

- (void)reportStatus:(OSStatus)status {
    if (status != _reportedStatus) {
        _reportedStatus = status;
        SWLogError([NSString stringWithFormat:@"decode the screen: %@", SWErrorMessageOf(SWErrorStatus(status))]);
    }
}

- (BOOL)adoptParameterSets:(SWAccessUnit *)unit {
    CMVideoFormatDescriptionRef described = SWHEVCFormat(unit.nals);
    if (described == NULL) {
        return YES;
    }
    BOOL adopted = YES;
    if (_format == NULL || !CMFormatDescriptionEqual(_format, described) || _session == NULL) {
        if (_format != NULL) {
            CFRelease(_format);
        }
        _format = (CMVideoFormatDescriptionRef)CFRetain(described);
        if (_session == NULL || !VTDecompressionSessionCanAcceptFormatDescription(_session, described)) {
            adopted = [self makeSession:described];
        }
    }
    CFRelease(described);
    return adopted;
}

- (BOOL)decode:(SWAccessUnit *)unit {
    if (atomic_exchange(&_failed, false)) {
        [self reportStatus:atomic_load(&_failedStatus)];
        [self invalidate];
    }
    if (unit.damaged) {
        _waitingForKeyframe = YES;
        return YES;
    }
    if (unit.hasParameterSets && ![self adoptParameterSets:unit]) {
        return YES;
    }
    if (_waitingForKeyframe) {
        if (!unit.isKeyframe) {
            return YES;
        }
        _waitingForKeyframe = NO;
    }
    if (_session == NULL && _format != NULL && ![self makeSession:_format]) {
        return YES;
    }
    if (_session == NULL || _format == NULL) {
        return NO;
    }
    CMSampleBufferRef sample = SWHEVCSample(unit, _format, [_clock time:unit.rtpTimestamp], kCMTimeInvalid);
    if (sample == NULL) {
        return NO;
    }
    __weak SWFrameDecoder *weakSelf = self;
    VTDecodeInfoFlags info = 0;
    OSStatus status = VTDecompressionSessionDecodeFrameWithOutputHandler(_session, sample, kVTDecodeFrame_1xRealTimePlayback | kVTDecodeFrame_EnableAsynchronousDecompression, &info,
        ^(OSStatus decoded, VTDecodeInfoFlags flags, CVImageBufferRef image, CMTime time, CMTime duration) {
            @autoreleasepool {
                SWFrameDecoder *decoder = weakSelf;
                if (decoder == nil) {
                    return;
                }
                if (decoded != noErr || image == NULL) {
                    if (!atomic_exchange(&decoder->_reportedDecodeError, true)) {
                        SWLogError([NSString stringWithFormat:@"video-diag decode callback OSStatus=%d flags=%u image_present=%d", (int)decoded, (unsigned int)flags, image != NULL]);
                    }
                    atomic_store(&decoder->_failedStatus, decoded);
                    atomic_store(&decoder->_failed, true);
                    return;
                }
                decoder.onFrame(image, unit);
            }
        });
    CFRelease(sample);
    if (status != noErr) {
        if (!atomic_exchange(&_reportedDecodeError, true)) {
            SWLogError([NSString stringWithFormat:@"video-diag decode submission OSStatus=%d", (int)status]);
        }
        [self reportStatus:status];
        [self invalidate];
        return YES;
    }
    return NO;
}

@end
