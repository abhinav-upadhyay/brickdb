/*-
 * Copyright (c) 2020 Abhinav Upadhyay
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

package logging

import (
	"log"
	"log/syslog"
)

type Logger struct {
	Alert, Crit, Debug, Emerg, Err, Info, Notice, Warning *log.Logger
}

func GetSysLog(priority syslog.Priority, flags int) *log.Logger {
	logger, err := syslog.NewLogger(priority, flags)
	if err != nil {
		panic(err)
	}
	return logger
}

func NewLogger(flags int) *Logger {
	return &Logger{
		Alert:   GetSysLog(syslog.LOG_ALERT, flags),
		Crit:    GetSysLog(syslog.LOG_CRIT, flags),
		Debug:   GetSysLog(syslog.LOG_DEBUG, flags),
		Emerg:   GetSysLog(syslog.LOG_EMERG, flags),
		Err:     GetSysLog(syslog.LOG_ERR, flags),
		Info:    GetSysLog(syslog.LOG_INFO, flags),
		Notice:  GetSysLog(syslog.LOG_NOTICE, flags),
		Warning: GetSysLog(syslog.LOG_WARNING, flags),
	}
}
