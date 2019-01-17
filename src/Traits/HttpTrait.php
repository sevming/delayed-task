<?php

namespace Sevming\DelayedTask\Traits;

trait HttpTrait
{
    /**
     * Request.
     *
     * @param string $url
     * @param array  $params
     * @param string $method
     * @param int    $timeout
     * @param array  $headers
     * @param array  $userAgent
     * @return mixed
     */
    public static function request(string $url, array $params = [], string $method = 'GET', int $timeout = 1, array $headers = [], array $userAgent = [])
    {
        if (!preg_match('/^(http|https)/is', $url)) {
            $url = 'http://' . $url;
        }

        $ch = curl_init();
        $method = strtoupper($method);
        switch ($method) {
            case 'GET':
                $url .= '?' . http_build_query($params);
                break;
            case 'POST':
                curl_setopt($ch, CURLOPT_POST, true);
                curl_setopt($ch, CURLOPT_POSTFIELDS, $params);
                break;
        }

        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);

        if ($timeout > 0 && $timeout < 1) {
            $timeout = (int)($timeout * 1000);
            curl_setopt($ch, CURLOPT_NOSIGNAL, true);
            curl_setopt($ch, CURLOPT_TIMEOUT_MS, $timeout);
        } else {
            $timeout = (int)$timeout;
            curl_setopt($ch, CURLOPT_TIMEOUT, $timeout);
        }

        if (!empty($headers)) {
            curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        }

        if (!empty($userAgent)) {
            curl_setopt($ch, CURLOPT_USERAGENT, $userAgent);
        }

        $result = curl_exec($ch);
        $errno = curl_errno($ch);
        curl_close($ch);
        if (!$errno) {
            return $result;
        }

        return false;
    }
}