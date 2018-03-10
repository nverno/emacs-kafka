;;; kafka-cli-services.el --- Summary -*- lexical-binding: t; -*-
;;; Commentary:
;;; Code:

(eval-and-compile 
  (require 'kafka-cli)
  (require 'cl-lib))
(autoload 'comint-check-proc "comint")

(eval-when-compile
  (defmacro kafka-buffer (type &optional proc)
    `(,(if proc 'get-buffer-process 'get-buffer)
      ,(intern (format "kafka-%s-buffer-name" type)))))

(defvar kafka-zookeeper-buffer-name "*zookeeper*")
(defvar kafka-broker-buffer-name "*kafka*")
(defvar kafka-consumer-buffer-name "*consumer*")

(defun kafka-zookeeper-cli-file-path ()
  "."
  (kafka-bin "zookeeper-server-start.sh"))

(defun kafka-zookeeper-cli-arguments ()
  "."
  (kafka-config "zookeeper.properties"))

(defun kafka-broker-cli-file-path ()
  "Path to the program used by `run-kafka'."
  (kafka-bin "kafka-server-start.sh"))

(defun kafka-broker-cli-arguments ()
  "Command line arguments to `kafka-server-start.sh'."
  (kafka-config "server.properties"))

(defun kafka-consumer-cli-file-path ()
  "Path to the program used by `run-kafka'."
  (kafka-bin "kafka-console-consumer.sh"))

(defun kafka-consumer-cli-arguments ()
  "Command line arguments to `kafka-console-consumer.sh'."
  `("--whitelist" ,kafka-consumer-whitelist-topics
    "--consumer-property" "group.id=kafka-cli-consumer1"
    "--bootstrap-server" ,kafka-url))

(defmacro kafka-define-runner (service doc)
  (declare (indent defun))
  (let ((name (intern (format "kafka-run-%s" service)))
        (bname (gensym)) (buff (gensym))
        (args (gensym)) (path (gensym)))
    (progn
      `(defun ,name (switch)
         ,doc
         (interactive "i")
         (let* ((,bname ,(intern (format "kafka-%s-buffer-name" service)))
                (,buff (get-buffer-create ,bname))
                (,args (,(intern (format "kafka-%s-cli-arguments" service))))
                (,path (,(intern (format "kafka-%s-cli-file-path" service)))))
           (if (comint-check-proc ,buff)
               (and switch (switch-to-buffer ,buff))
             (apply 'make-comint-in-buffer ,bname ,buff ,path nil (list ,args))
             (and switch (switch-to-buffer ,buff)))
           (with-current-buffer ,buff
             (kafka-cli-log-mode)))))))

(kafka-define-runner "zookeeper"
  "Run zookeeper swith to buffer if SWITCH is non nil.")

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
            (kafka-buffer consumer t) 'kafka-consumer-sentinel)
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
      (signal-process (kafka-buffer consumer t) 18)))

(defun kafka-services-running ()
  "Return true if all the kafka services are running."
  ;; this has race conditions don't rely on this.
  (cl-every 'comint-check-proc '(kafka-zookeeper-buffer-name
                                 kafka-broker-buffer-name
                                 kafka-consumer-buffer-name)))

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
