;;; kafka-cli-services.el --- Summary -*- lexical-binding: t; -*-
;;; Commentary:
;;; Code:

(eval-when-compile 
  (require 'cl-lib)
  (require 'magit-popup)
  (require 'kafka-cli))
(require 'kafka-cli)

(autoload 'comint-check-proc "comint")

(defcustom kafka-consumer-whitelist-topics "(.*)"
  "List of topics to consume from."
  :type 'string
  :group 'kafka-cli)

(defvar kafka-consumer-cli-arguments
  (list "--whitelist" kafka-consumer-whitelist-topics
        "--consumer-property" "group.id=kafka-cli-consumer1"
        "--bootstrap-server" kafka-url)
  "Command line arguments to `kafka-console-consumer.sh'.")

(eval-when-compile
  (cl-defmacro kafka-define-runner (service doc &key cli-props cli-path)
    "Define functions to run kafka services"
    (declare (indent defun))
    (declare-function kafka-cli-log-mode "kafka-cli")
    (let* ((name (intern (format "kafka-run-%s" service)))
           (bname (gensym))
           (buff (gensym))
           (args (gensym))
           (path (gensym)))
      (progn
        `(defun ,name (switch)
           ,doc
           (interactive "i")
           (let* ((,bname ,(kafka-buffer service :name t))
                  (,buff ,(kafka-buffer service))
                  (,args ,cli-props)
                  (,path ,cli-path))
             (if (comint-check-proc ,buff)
                 (and switch (switch-to-buffer ,buff))
               (apply 'make-comint-in-buffer ,bname ,buff ,path nil (list ,args))
               (and switch (switch-to-buffer ,buff)))
             (with-current-buffer ,buff
               (kafka-cli-log-mode))))))))

;;; FIXME: macro in macro expansion
;;;###autoload(autoload 'kafka-run-zookeeper "kafka-cli-services")
(kafka-define-runner "zookeeper"
  "Run zookeeper switch to buffer if SWITCH is non nil"
  :cli-props (kafka-config "zookeeper.properties")
  :cli-path (kafka-bin "zookeeper-server-start.sh"))    

(kafka-define-runner "broker"
  "Run kafka Broker, swith to buffer if SWITCH is non nil.")

(kafka-define-runner "consumer"
  "Run Kafka Consumre, switch to buffer if SWITCH is non nil.")

(defun kafka-consumer-sentinel (_process _event)
  "PROCESS EVENT ."
  (kafka-run-consumer 1))

(defun kafka-restart-consumer (switch)
  "Kill the process, and start again, switch to buffer if SWITCH is non 'nil."
  (interactive "i")
  (if (comint-check-proc (kafka-buffer consumer))
      (kafka-buffer consumer)
    (progn (set-process-sentinel
            (kafka-buffer consumer :proc t) 'kafka-consumer-sentinel)
	   (interrupt-process (kafka-buffer consumer)))
    (kafka-run-consumer switch)))

(defun kafka-pause-consumer ()
  "."
  (interactive)
  (if (comint-check-proc (kafka-buffer consumer))
      (kafka-buffer consumer)
    (signal-process (kafka-buffer consumer t) 19)))

(defun kafka-continue-consumer ()
  "."
  (interactive)
  (if (comint-check-proc (kafka-buffer consumer))
      (signal-process (kafka-buffer consumer :proc t) 18)))

(defun kafka-services-running ()
  "Return true if all the kafka services are running."
  ;; this has race conditions don't rely on this.
  (cl-every 'comint-check-proc `(,(kafka-buffer zookeeper :name t)
                                 ,(kafka-buffer broker :name t)
                                 ,(kafka-buffer consumber :name t))))

(defun kafka-start-all-services ()
  "Start all services."
  (kafka-run-zookeeper 1) ; race condition fix this.
  (kafka-run-broker 1)
  (kafka-run-consumer 1))

;;; FIXME: why is this checked on load
(and (not (comint-check-proc (get-buffer kafka-broker-buffer-name)))
     (user-error "Kafka broker not running"))

(provide 'kafka-cli-services)

;;; kafka-cli-services.el ends here
