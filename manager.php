<?php
namespace Sssr\Cashback\Wd;

use Sssr\Cashback\Core;
use Sssr\Cashback\User;
use Sssr\Cashback\Tools;
use Bitrix\Main\Application;
use Bitrix\Main\Localization\Loc;
Loc::loadMessages(__FILE__);

class Manager
{

	const WITHDRAW_TYPE_CARD = 1;

	const WITHDRAW_TYPE_YM = 2;

	const WITHDRAW_TYPE_PHONE = 3;

	const WITHDRAW_TYPE_WM = 4;

	const WITHDRAW_TYPE_PP = 5;

	const WITHDRAW_TYPE_QIWI = 6;

	const WITHDRAW_SYSTEM_TYPE_YANDEX = 1;

	const WITHDRAW_SYSTEM_TYPE_PAYEER = 2;

	const WITHDRAW_SYSTEM_TYPE_PAYU = 3;

	const WITHDRAW_MAX_ATTEMPT_COUNT = 35;

	public static $arWithdrawSystemClass = array(
		self::WITHDRAW_SYSTEM_TYPE_YANDEX => __NAMESPACE__ . '\\Yandex',
		self::WITHDRAW_SYSTEM_TYPE_PAYEER => __NAMESPACE__ . '\\Payeer',
		self::WITHDRAW_SYSTEM_TYPE_PAYU => __NAMESPACE__ . '\\Payu'
	);

	public static $arWithdrawTypesNames = array(
		self::WITHDRAW_TYPE_CARD => 'CARD',
		self::WITHDRAW_TYPE_YM => 'YM',
		self::WITHDRAW_TYPE_PHONE => 'PHONE',
		self::WITHDRAW_TYPE_WM => 'WM',
		self::WITHDRAW_TYPE_PP => 'PP',
		self::WITHDRAW_TYPE_QIWI => 'QIWI'
	);

	public static $arWithdrawTypesForAuto = array(
		self::WITHDRAW_TYPE_YM => self::WITHDRAW_SYSTEM_TYPE_YANDEX,
		self::WITHDRAW_TYPE_PHONE => self::WITHDRAW_SYSTEM_TYPE_YANDEX,
		self::WITHDRAW_TYPE_QIWI => self::WITHDRAW_SYSTEM_TYPE_PAYEER,
		self::WITHDRAW_TYPE_CARD => self::WITHDRAW_SYSTEM_TYPE_PAYU
	);

	const WITHDRAW_ALL_TIME_LIMIT = 40;

	public static function withdrawAll($bAgent = true)
	{
		$uniq = '\Sssr\Cashback\Wd\Manager::withdrawAll.' . \CMain::GetServerUniqID();
		
		if (! \Bitrix\Main\Loader::includeModule("iblock") || ! \Bitrix\Main\Loader::includeModule("currency") || ! Tools::getLock($uniq, 0))
		{
			if ($bAgent) return '\Sssr\Cashback\Wd\Manager::withdrawAll();';
			return;
		}
		
		@set_time_limit(0);
		@ignore_user_abort(true);
		
		$iStartTime = time();
		
		$bWithdrawSystemDebug = false;
		
		$obConnection = Application::getConnection();
		$obSqlHelper = $obConnection->getSqlHelper();
		
		$sBaseCurrency = \CCurrency::GetBaseCurrency();
		$arBaseCurrencyFormat = \CCurrencyLang::GetCurrencyFormat($sBaseCurrency);
		
		$iWithdrawIBlockId = \COption::GetOptionInt(Core::MODULE_ID, 'withdraw_iblock_id');
		
		$iWithdrawAutoDelay = max(0, intval(\COption::GetOptionInt(Core::MODULE_ID, 'withdraw_auto_delay')));
		
		$fWithdrawMaxSum = round(max(0, floatval(str_replace(',', '.', \COption::GetOptionString(Core::MODULE_ID, 'user_withdraw_max_sum')))), $arBaseCurrencyFormat['DECIMALS']);
		
		$arStatusXmlIds = Tools::getIBlockPropertyEnumXmlIds('STATUS', $iWithdrawIBlockId);
		
		$arTypeXmlIds = Tools::getIBlockPropertyEnumXmlIds('WITHDRAW_TYPE', $iWithdrawIBlockId);
		
		$obConnection->query('update `b_sssr_cashback_withdraw` set `IS_LOCK`=0 where `IS_LOCK`=1 and `LOCK_DATETIME` < date_add(now(),interval -600 SECOND)');
		
		$arElFilter = array(
			'IBLOCK_ID' => $iWithdrawIBlockId,
			'PROPERTY_STATUS' => $arStatusXmlIds['NEW']
		);
		
		if ($iWithdrawAutoDelay > 0) $arElFilter['<=DATE_CREATE'] = ConvertTimeStamp(time() - $iWithdrawAutoDelay, 'FULL');
		
		$arTypesForAutoIds = array();
		
		if (count(self::$arWithdrawTypesForAuto))
		{
			foreach (self::$arWithdrawTypesForAuto as $iTFA => $iTFAS)
			{
				if (isset(self::$arWithdrawTypesNames[$iTFA]) && isset($arTypeXmlIds[self::$arWithdrawTypesNames[$iTFA]])) $arTypesForAutoIds[] = $arTypeXmlIds[self::$arWithdrawTypesNames[$iTFA]];
			}
			
			if (count($arTypesForAutoIds) < 1) $arTypesForAutoIds[] = 0;
		}
		
		if (count($arTypesForAutoIds)) $arElFilter['PROPERTY_WITHDRAW_TYPE'] = $arTypesForAutoIds;
		
		if ($rsElements = \CIBlockElement::GetList(array(
			'ID' => 'ASC'
		), $arElFilter, false, array(
			'nTopCount' => 10000
		), array(
			'ID',
			'IBLOCK_ID'
		)))
		{
			$arWithdrawElIds = array();
			
			while ($rsFields = $rsElements->GetNextElement(true, false))
			{
				$arElement = $rsFields->GetFields();
				
				$arWithdrawElIds[] = $arElement['ID'];
			}
			
			if (count($arWithdrawElIds))
			{
				$obConnection->query('insert into `b_sssr_cashback_withdraw`(`WITHDRAW_OBJECT_ID`)values(' . implode('),(', $arWithdrawElIds) . ') ON DUPLICATE KEY UPDATE `IS_END`=if(`ATTEMPT_COUNT`<' . self::WITHDRAW_MAX_ATTEMPT_COUNT . ',0,`IS_END`)');
			}
		}
		
		while (true)
		{
			// 0 check time limit -------------------------------------
			
			if ((time() - $iStartTime) >= self::WITHDRAW_ALL_TIME_LIMIT) break;
			
			// 1 Lock -------------------------------------------------
			
			$obConnection->startTransaction();
			
			$query = 'select w.`ID`,w.`WITHDRAW_OBJECT_ID`,r.`ID` as `REQUEST_ID`,r.`IS_CURRENT` as `REQUEST_IS_CURRENT` from `b_sssr_cashback_withdraw` as w left join `b_sssr_cashback_withdraw_request` as r on r.`ID`=w.`LAST_REQUEST_ID`' . ' where w.`IS_END`=0 and w.`ATTEMPT_NEXT_DATETIME` < now() and w.`IS_LOCK`=0 limit 1 FOR UPDATE';
			
			if (! ($rsWithdraw = $obConnection->query($query)) || ! ($arWithdraw = $rsWithdraw->fetch()))
			{
				$obConnection->rollbackTransaction();
				break;
			}
			
			if (! $arWithdraw['REQUEST_ID'] || ! $arWithdraw['REQUEST_IS_CURRENT'])
			{
				\CIBlockElement::SetPropertyValuesEx($arWithdraw['WITHDRAW_OBJECT_ID'], $iWithdrawIBlockId, array(
					'DENY_EDIT' => 1
				));
			}
			
			$obConnection->query('update `b_sssr_cashback_withdraw` set `IS_LOCK`=1,`LOCK_DATETIME`=now() where `ID`=' . $arWithdraw['ID']);
			
			$obConnection->commitTransaction();
			
			// 2 prepare and save request -------------------------------------------------
			
			$obConnection->startTransaction();
			
			$query = 'select w.`ID`,w.`WITHDRAW_OBJECT_ID`,w.`ATTEMPT_COUNT`,r.`ID` as `REQUEST_ID`,r.`IS_CURRENT` as `REQUEST_IS_CURRENT`,r.`SYSTEM_TYPE` as `REQUEST_SYSTEM_TYPE`,r.`USER_ID` as `REQUEST_USER_ID`,r.`SUM` as `REQUEST_SUM`,r.`WITHDRAW_TYPE` as `REQUEST_WITHDRAW_TYPE`,r.`WITHDRAW_PARAMS` as `REQUEST_WITHDRAW_PARAMS`,r.`REQUEST_PARAMS` as `REQUEST_REQUEST_PARAMS` from `b_sssr_cashback_withdraw` as w left join `b_sssr_cashback_withdraw_request` as r on r.`ID`=w.`LAST_REQUEST_ID`' . ' where w.`ID`=' . $arWithdraw['ID'] . ' FOR UPDATE';
			
			if (! ($rsWithdraw = $obConnection->query($query)) || ! ($arWithdraw = $rsWithdraw->fetch()))
			{
				$obConnection->rollbackTransaction();
				
				$obConnection->query('update `b_sssr_cashback_withdraw` set `IS_LOCK`=0,`ATTEMPT_NEXT_DATETIME`=date_add(now(),interval ' . intval(mt_rand(100, 500)) . ' SECOND) where `ID`=' . $arWithdraw['ID']);
				
				continue;
			}
			
			if ($arWithdraw['REQUEST_ID'] && $arWithdraw['REQUEST_IS_CURRENT'])
			{
				// update request
				
				$arForRequest = array();
				
				$arForRequest['bIsNewRequest'] = false;
				$arForRequest['REQUEST_ID'] = $arWithdraw['REQUEST_ID'];
				$arForRequest['SYSTEM_TYPE'] = $arWithdraw['REQUEST_SYSTEM_TYPE'];
				$arForRequest['USER_ID'] = $arWithdraw['REQUEST_USER_ID'];
				$arForRequest['SUM'] = $arWithdraw['REQUEST_SUM'];
				$arForRequest['WITHDRAW_TYPE'] = $arWithdraw['REQUEST_WITHDRAW_TYPE'];
				$arForRequest['WITHDRAW_PARAMS'] = strlen($arWithdraw['REQUEST_WITHDRAW_PARAMS']) < 1 ? array() : unserialize($arWithdraw['REQUEST_WITHDRAW_PARAMS']);
				$arForRequest['REQUEST_PARAMS'] = strlen($arWithdraw['REQUEST_REQUEST_PARAMS']) < 1 ? array() : unserialize($arWithdraw['REQUEST_REQUEST_PARAMS']);
				
				if (! isset(self::$arWithdrawSystemClass[$arForRequest['SYSTEM_TYPE']]) || ! class_exists(($sWithdrawSystemClass = self::$arWithdrawSystemClass[$arForRequest['SYSTEM_TYPE']])))
				{
					$obConnection->query('update `b_sssr_cashback_withdraw` set `IS_END`=1,`IS_LOCK`=0,`ATTEMPT_NEXT_DATETIME`=date_add(now(),interval ' . intval(mt_rand(100, 500)) . ' SECOND) where `ID`=' . $arWithdraw['ID']);
					
					\CIBlockElement::SetPropertyValuesEx($arWithdraw['WITHDRAW_OBJECT_ID'], $iWithdrawIBlockId, array(
						'DENY_EDIT' => 0
					));
					
					$obConnection->commitTransaction();
					
					continue;
				}
				
				switch ($arForRequest['SYSTEM_TYPE'])
				{
					case self::WITHDRAW_SYSTEM_TYPE_YANDEX:
						
						$arWithdrawSystemSettings = array();
						$arWithdrawSystemSettings['agentid'] = $arForRequest['REQUEST_PARAMS']['agentid'];
						$arWithdrawSystemSettings['server'] = trim(\COption::GetOptionString(Core::MODULE_ID, $bWithdrawSystemDebug ? 'wd_yandex_t_server' : 'wd_yandex_server'));
						
						$arWithdrawSystemSettings['certificate_path'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_yandex_cert_path'));
						$arWithdrawSystemSettings['certificate_private_key_path'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_yandex_cert_key_path'));
						$arWithdrawSystemSettings['certificate_password'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_yandex_cert_password'));
						$arWithdrawSystemSettings['certificate_r_path'] = trim(\COption::GetOptionString(Core::MODULE_ID, $bWithdrawSystemDebug ? 'wd_yandex_cert_r_t_path' : 'wd_yandex_cert_r_path'));
						
						$obWithdrawSystem = new $sWithdrawSystemClass($arWithdrawSystemSettings, $bWithdrawSystemDebug);
					break;
					case self::WITHDRAW_SYSTEM_TYPE_PAYEER:
						
						$arWithdrawSystemSettings = array();
						
						$arWithdrawSystemSettings['account'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_payeer_account'));
						$arWithdrawSystemSettings['api_id'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_payeer_api_id'));
						$arWithdrawSystemSettings['api_pass'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_payeer_api_pass'));
						
						$obWithdrawSystem = new $sWithdrawSystemClass($arWithdrawSystemSettings, $bWithdrawSystemDebug);
					break;
					case self::WITHDRAW_SYSTEM_TYPE_PAYU:
						
						$arWithdrawSystemSettings = array();
						
						$arWithdrawSystemSettings['merchant_code'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_payu_merchant_code'));
						$arWithdrawSystemSettings['secret_key'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_payu_secret_key'));
						
						$obWithdrawSystem = new $sWithdrawSystemClass($arWithdrawSystemSettings, $bWithdrawSystemDebug);
					break;
					default:
				}
				
				$obConnection->query('update `b_sssr_cashback_withdraw_request` set `ATTEMPT_DATETIME`=now(),`ATTEMPT_COUNT`=`ATTEMPT_COUNT`+1 where `ID`=' . $arForRequest['REQUEST_ID']);
			}
			else
			{
				// new request
				
				$bForNoRequest = false;
				
				if (! ($rsWithdrawElement = \CIBlockElement::GetList(array(
					'ID' => 'ASC'
				), array(
					'IBLOCK_ID' => $iWithdrawIBlockId,
					'ID' => $arWithdraw['WITHDRAW_OBJECT_ID']
				), false, array(
					'nTopCount' => 1
				), array(
					'ID',
					'IBLOCK_ID',
					'PROPERTY_USER',
					'PROPERTY_STATUS',
					'PROPERTY_SUM',
					'PROPERTY_WITHDRAW_TYPE',
					'PROPERTY_UF_WD_CARD_NUMBER',
					'PROPERTY_UF_WD_YM_0',
					'PROPERTY_UF_WD_PHONE',
					'PROPERTY_UF_WD_WM_0',
					'PROPERTY_UF_WD_PP_EMAIL',
					'PROPERTY_UF_WD_QIWI_0'
				))) || ! ($rsFields = $rsWithdrawElement->GetNextElement()) || ! ($arWithdrawElement = $rsFields->GetFields()) || ! $arWithdrawElement['ID'] || $arWithdrawElement['PROPERTY_STATUS_ENUM_ID'] != $arStatusXmlIds['NEW'] || ($sSearchTypeXmlName = array_search($arWithdrawElement['PROPERTY_WITHDRAW_TYPE_ENUM_ID'], $arTypeXmlIds)) === false || ($iCurWithdrawTypeId = array_search($sSearchTypeXmlName, self::$arWithdrawTypesNames)) === false || ! isset(self::$arWithdrawTypesForAuto[$iCurWithdrawTypeId]) || ! isset(self::$arWithdrawSystemClass[self::$arWithdrawTypesForAuto[$iCurWithdrawTypeId]]) || ! class_exists(($sWithdrawSystemClass = self::$arWithdrawSystemClass[self::$arWithdrawTypesForAuto[$iCurWithdrawTypeId]])))
				{
					$bForNoRequest = true;
				}
				
				if (! $bForNoRequest)
				{
					$fForWithdrawSum = round(max(0, floatval(str_replace(',', '.', $arWithdrawElement['PROPERTY_SUM_VALUE']))), $arBaseCurrencyFormat['DECIMALS']);
					
					if ($fForWithdrawSum < 0.00001 || $fForWithdrawSum > $fWithdrawMaxSum)
					{
						$bForNoRequest = true;
					}
				}
				
				if ($bForNoRequest)
				{
					$obConnection->query('update `b_sssr_cashback_withdraw` set `IS_END`=1,`IS_LOCK`=0,`ATTEMPT_NEXT_DATETIME`=date_add(now(),interval ' . intval(mt_rand(100, 500)) . ' SECOND) where `ID`=' . $arWithdraw['ID']);
					
					\CIBlockElement::SetPropertyValuesEx($arWithdraw['WITHDRAW_OBJECT_ID'], $iWithdrawIBlockId, array(
						'DENY_EDIT' => 0
					));
					
					$obConnection->commitTransaction();
					
					continue;
				}
				
				$iForWithdrawUserId = intval($arWithdrawElement['PROPERTY_USER_VALUE']);
				
				$bIsLockAccount = false;
				if ($iForWithdrawUserId && (! ($bIsLockAccount = User::lockAccount($iForWithdrawUserId)) || ! ($arAccount = User::getAccount($iForWithdrawUserId)) || $fForWithdrawSum > round($arAccount['CURRENT_BUDGET'], $arBaseCurrencyFormat['DECIMALS']) || ! User::AccountPay($iForWithdrawUserId, $fForWithdrawSum, User::ACCOUNT_TRANSACTION_TYPE_WITHDRAW, array(
					'OBJECT_ID' => $arWithdraw['WITHDRAW_OBJECT_ID']
				))))
				{
					if ($bIsLockAccount) User::unlockAccount($iForWithdrawUserId);
					
					$obConnection->query('update `b_sssr_cashback_withdraw` set `IS_LOCK`=0,`ATTEMPT_NEXT_DATETIME`=date_add(now(),interval ' . intval(mt_rand(3600, 36000)) . ' SECOND) where `ID`=' . $arWithdraw['ID']);
					
					\CIBlockElement::SetPropertyValuesEx($arWithdraw['WITHDRAW_OBJECT_ID'], $iWithdrawIBlockId, array(
						'DENY_EDIT' => 0
					));
					
					$obConnection->commitTransaction();
					
					continue;
				}
				
				if ($bIsLockAccount) User::unlockAccount($iForWithdrawUserId);
				
				$arForRequest = array();
				
				$arForRequest['bIsNewRequest'] = true;
				
				$arForRequest['REQUEST_ID'] = 0;
				$arForRequest['WITHDRAW_TYPE'] = $iCurWithdrawTypeId;
				$arForRequest['SYSTEM_TYPE'] = self::$arWithdrawTypesForAuto[$iCurWithdrawTypeId];
				$arForRequest['USER_ID'] = $iForWithdrawUserId;
				$arForRequest['SUM'] = $fForWithdrawSum;
				$arForRequest['WITHDRAW_PARAMS'] = array();
				$arForRequest['REQUEST_PARAMS'] = array();
				
				switch ($arForRequest['SYSTEM_TYPE'])
				{
					case self::WITHDRAW_SYSTEM_TYPE_YANDEX:
						
						switch ($arForRequest['WITHDRAW_TYPE'])
						{
							case self::WITHDRAW_TYPE_YM:
								$arForRequest['REQUEST_PARAMS']['dstAccount'] = trim($arWithdrawElement['PROPERTY_UF_WD_YM_0_VALUE']);
							break;
							case self::WITHDRAW_TYPE_PHONE:
								$arForRequest['REQUEST_PARAMS']['dstAccount'] = trim($arWithdrawElement['PROPERTY_UF_WD_PHONE_VALUE']);
							break;
							default:
						}
						
						$arForRequest['REQUEST_PARAMS']['contract'] = Loc::getMessage('CASHBACK_WD_MANAGER_SYSTEM_CONTRACT_DEFAULT');
						
						$arWithdrawSystemSettings = array();
						$arWithdrawSystemSettings['agentid'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_yandex_agentid'));
						$arWithdrawSystemSettings['server'] = trim(\COption::GetOptionString(Core::MODULE_ID, $bWithdrawSystemDebug ? 'wd_yandex_t_server' : 'wd_yandex_server'));
						
						$arWithdrawSystemSettings['certificate_path'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_yandex_cert_path'));
						$arWithdrawSystemSettings['certificate_private_key_path'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_yandex_cert_key_path'));
						$arWithdrawSystemSettings['certificate_password'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_yandex_cert_password'));
						$arWithdrawSystemSettings['certificate_r_path'] = trim(\COption::GetOptionString(Core::MODULE_ID, $bWithdrawSystemDebug ? 'wd_yandex_cert_r_t_path' : 'wd_yandex_cert_r_path'));
						
						$arForRequest['REQUEST_PARAMS']['agentid'] = $arWithdrawSystemSettings['agentid'];
						$arForRequest['REQUEST_PARAMS']['server'] = $arWithdrawSystemSettings['server'];
						
						$obWithdrawSystem = new $sWithdrawSystemClass($arWithdrawSystemSettings, $bWithdrawSystemDebug);
					break;
					case self::WITHDRAW_SYSTEM_TYPE_PAYEER:
						
						switch ($arForRequest['WITHDRAW_TYPE'])
						{
							case self::WITHDRAW_TYPE_QIWI:
								$arForRequest['REQUEST_PARAMS']['param_ACCOUNT_NUMBER'] = trim($arWithdrawElement['PROPERTY_UF_WD_QIWI_0_VALUE']);
							break;
							default:
						}
						
						$arWithdrawSystemSettings = array();
						$arWithdrawSystemSettings['account'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_payeer_account'));
						$arWithdrawSystemSettings['api_id'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_payeer_api_id'));
						$arWithdrawSystemSettings['api_pass'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_payeer_api_pass'));
						
						$arForRequest['REQUEST_PARAMS']['account'] = $arWithdrawSystemSettings['account'];
						$arForRequest['REQUEST_PARAMS']['apiId'] = $arWithdrawSystemSettings['api_id'];
						$arForRequest['REQUEST_PARAMS']['apiPass'] = $arWithdrawSystemSettings['api_pass'];
						
						$obWithdrawSystem = new $sWithdrawSystemClass($arWithdrawSystemSettings, $bWithdrawSystemDebug);
					break;
					case self::WITHDRAW_SYSTEM_TYPE_PAYU:
						
						switch ($arForRequest['WITHDRAW_TYPE'])
						{
							case self::WITHDRAW_TYPE_CARD:
								$arForRequest['REQUEST_PARAMS']['ccnumber'] = trim($arWithdrawElement['PROPERTY_UF_WD_CARD_NUMBER_VALUE']);
							break;
							default:
						}
						
						$arWithdrawSystemSettings = array();
						$arWithdrawSystemSettings['merchant_code'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_payu_merchant_code'));
						$arWithdrawSystemSettings['secret_key'] = trim(\COption::GetOptionString(Core::MODULE_ID, 'wd_payu_secret_key'));
						
						$arForRequest['REQUEST_PARAMS']['merchant_code'] = $arWithdrawSystemSettings['merchant_code'];
						$arForRequest['REQUEST_PARAMS']['secret_key'] = $arWithdrawSystemSettings['secret_key'];
						
						$obWithdrawSystem = new $sWithdrawSystemClass($arWithdrawSystemSettings, $bWithdrawSystemDebug);
					break;
					default:
				}
				
				$query = 'insert ignore into `b_sssr_cashback_withdraw_request`(`SYSTEM_TYPE`,`WITHDRAW_ID`,`IS_CURRENT`,`USER_ID`,`SUM`,`WITHDRAW_TYPE`,`WITHDRAW_PARAMS`,`REQUEST_PARAMS`,`IS_SUCCESS`,`ATTEMPT_DATETIME`,`ATTEMPT_COUNT`)values(' . $arForRequest['SYSTEM_TYPE'] . ',' . $arWithdraw['ID'] . ',1,' . $arForRequest['USER_ID'] . ',' . $arForRequest['SUM'] . ',' . $arForRequest['WITHDRAW_TYPE'] . ',"' . (count($arForRequest['WITHDRAW_PARAMS']) > 0 ? $obSqlHelper->forSql(serialize($arForRequest['WITHDRAW_PARAMS'])) : '') . '"' . ',"' . (count($arForRequest['REQUEST_PARAMS']) > 0 ? $obSqlHelper->forSql(serialize($arForRequest['REQUEST_PARAMS'])) : '') . '"' . ',0,now(),1)';
				$obConnection->query($query);
				
				if (($iNewWithdrawRequestId = intval($obConnection->getInsertedId())) < 1)
				{
					$bIsLockAccount = false;
					
					if ($arForRequest['USER_ID'] && (! ($bIsLockAccount = User::lockAccount($arForRequest['USER_ID'])) || ! User::AccountPay($arForRequest['USER_ID'], - $arForRequest['SUM'], User::ACCOUNT_TRANSACTION_TYPE_WITHDRAW_RETURN, array(
						'OBJECT_ID' => $arWithdraw['WITHDRAW_OBJECT_ID']
					))))
					{
						$obConnection->rollbackTransaction();
						
						continue;
					}
					
					if ($bIsLockAccount) User::unlockAccount($arForRequest['USER_ID']);
					
					$obConnection->query('update `b_sssr_cashback_withdraw` set `IS_LOCK`=0,`ATTEMPT_NEXT_DATETIME`=date_add(now(),interval ' . intval(mt_rand(10, 30)) . ' SECOND) where `ID`=' . $arWithdraw['ID']);
					
					\CIBlockElement::SetPropertyValuesEx($arWithdraw['WITHDRAW_OBJECT_ID'], $iWithdrawIBlockId, array(
						'DENY_EDIT' => 0
					));
					
					$obConnection->commitTransaction();
					
					continue;
				}
				
				$arForRequest['REQUEST_ID'] = $iNewWithdrawRequestId;
			}
			
			$iAttemptDelay = $obWithdrawSystem->getAttemptDelay($arWithdraw['ATTEMPT_COUNT']);
			
			$obConnection->query('update `b_sssr_cashback_withdraw` set `LAST_REQUEST_ID`=' . $arForRequest['REQUEST_ID'] . ',`ATTEMPT_DATETIME`=now(),`ATTEMPT_COUNT`=`ATTEMPT_COUNT`+1,`ATTEMPT_NEXT_DATETIME`=date_add(now(),interval ' . intval($iAttemptDelay) . ' SECOND) where `ID`=' . $arWithdraw['ID']);
			
			$obConnection->commitTransaction();
			
			// 3 send request -------------------------------------------------
			
			$arRequestResult = $obWithdrawSystem->doWithdrawRequest($arForRequest['REQUEST_ID'], $arForRequest['SUM'], $arForRequest['WITHDRAW_TYPE'], $arForRequest['REQUEST_PARAMS'], $arForRequest['WITHDRAW_PARAMS']);
			
			if ($arRequestResult['IS_END']) $arRequestResult['IS_CURRENT'] = false;
			
			$bIsEndMaxAttemptCount = false;
			
			if (! $arRequestResult['IS_END'] && ($arWithdraw['ATTEMPT_COUNT'] + 1) >= self::WITHDRAW_MAX_ATTEMPT_COUNT)
			{
				$arRequestResult['IS_END'] = true;
				$arRequestResult['IS_CURRENT'] = false;
				$bIsEndMaxAttemptCount = true;
			}
			
			if (isset($arRequestResult['WITHDRAW_PARAMS']))
			{
				$obConnection->query('update `b_sssr_cashback_withdraw_request` set `WITHDRAW_PARAMS`="' . (count($arRequestResult['WITHDRAW_PARAMS']) > 0 ? $obSqlHelper->forSql(serialize($arRequestResult['WITHDRAW_PARAMS'])) : '') . '" where `ID`=' . $arForRequest['REQUEST_ID']);
			}
			
			$obConnection->startTransaction();
			
			$bIsLockAccount = false;
			if ($arForRequest['USER_ID'] && ! $arRequestResult['IS_CURRENT'] && ! $arRequestResult['IS_SUCCESS'] && (! ($bIsLockAccount = User::lockAccount($arForRequest['USER_ID'])) || ! User::AccountPay($arForRequest['USER_ID'], - $arForRequest['SUM'], User::ACCOUNT_TRANSACTION_TYPE_WITHDRAW_RETURN, array(
				'OBJECT_ID' => $arWithdraw['WITHDRAW_OBJECT_ID']
			))))
			{
				if ($bIsLockAccount) User::unlockAccount($arForRequest['USER_ID']);
				
				$obConnection->query('update `b_sssr_cashback_withdraw` set `IS_LOCK`=0 where `ID`=' . $arWithdraw['ID']);
				
				$obConnection->commitTransaction();
				
				continue;
			}
			
			if ($bIsLockAccount) User::unlockAccount($arForRequest['USER_ID']);
			
			$obConnection->query('update `b_sssr_cashback_withdraw_request` set `IS_CURRENT`=' . ($arRequestResult['IS_CURRENT'] ? 1 : 0) . ',`IS_SUCCESS`=' . ($arRequestResult['IS_SUCCESS'] ? 1 : 0) . ',`ERROR_CODE`="' . $obSqlHelper->forSql($arRequestResult['ERROR_CODE']) . '",`ERROR_DESCRIPTION`="' . $obSqlHelper->forSql($arRequestResult['ERROR_DESCRIPTION']) . '" where `ID`=' . $arForRequest['REQUEST_ID']);
			
			$obConnection->query('update `b_sssr_cashback_withdraw` set `IS_LOCK`=0,`IS_END`=' . ($arRequestResult['IS_END'] ? 1 : 0) . ($arRequestResult['IS_END'] && ! $arRequestResult['IS_SUCCESS'] ? ',`ATTEMPT_NEXT_DATETIME`=date_add(now(),interval ' . intval(mt_rand(3600, 36000)) . ' SECOND)' : '') . ' where `ID`=' . $arWithdraw['ID']);
			
			if ($arRequestResult['IS_SUCCESS'])
			{
				\CIBlockElement::SetPropertyValuesEx($arWithdraw['WITHDRAW_OBJECT_ID'], $iWithdrawIBlockId, array(
					'DENY_EDIT' => 1,
					'STATUS' => $arStatusXmlIds['SUCCESS']
				));
			}
			else
			{
				if (! $arRequestResult['IS_CURRENT'])
				{
					$arUpdVls = array(
						'DENY_EDIT' => 0
					);
					
					if ($arRequestResult['IS_END'] && ! $bIsEndMaxAttemptCount) $arUpdVls['STATUS'] = $arStatusXmlIds['REJECTED'];
					
					\CIBlockElement::SetPropertyValuesEx($arWithdraw['WITHDRAW_OBJECT_ID'], $iWithdrawIBlockId, $arUpdVls);
				}
			}
			
			$obConnection->commitTransaction();
		}
		
		Tools::releaseLock($uniq);
		
		if ($bAgent) return '\Sssr\Cashback\Wd\Manager::withdrawAll();';
	}

	public static function rejectWithdrawRequest($iWithdrawRequestId, $iCheckSystemType = false, $sErrorCode = '', $sErrorDescription = '')
	{
		$arResult = array();
		$arResult['IS_SUCCESS'] = false;
		
		$iWithdrawRequestId = intval($iWithdrawRequestId);
		
		if ($iCheckSystemType) $iCheckSystemType = intval($iCheckSystemType);
		
		$obConnection = Application::getConnection();
		$obSqlHelper = $obConnection->getSqlHelper();
		
		$obConnection->query('update `b_sssr_cashback_withdraw` set `IS_LOCK`=0 where `IS_LOCK`=1 and `LOCK_DATETIME` < date_add(now(),interval -600 SECOND)');
		
		$obConnection->startTransaction();
		
		$query = 'select r.`WITHDRAW_ID`,r.`SYSTEM_TYPE`,r.`IS_CURRENT`,r.`USER_ID`,r.`SUM`,r.`IS_SUCCESS`,w.`IS_LOCK`,w.`WITHDRAW_OBJECT_ID` from `b_sssr_cashback_withdraw_request` as r inner join `b_sssr_cashback_withdraw` as w on w.`ID`=r.`WITHDRAW_ID`' . ' where r.`ID`=' . $iWithdrawRequestId . ' FOR UPDATE';
		
		if (! ($rsWithdrawRequest = $obConnection->query($query)) || ! ($arWithdrawRequest = $rsWithdrawRequest->fetch()) || $arWithdrawRequest['IS_LOCK'] || $arWithdrawRequest['IS_CURRENT'] || ! $arWithdrawRequest['IS_SUCCESS'] || ($iCheckSystemType && $iCheckSystemType != $arWithdrawRequest['SYSTEM_TYPE']))
		{
			$obConnection->rollbackTransaction();
			
			if (! $arWithdrawRequest['IS_SUCCESS'] && ! ($iCheckSystemType && $iCheckSystemType != $arWithdrawRequest['SYSTEM_TYPE'])) $arResult['IS_SUCCESS'] = true;
			
			return $arResult;
		}
		
		$bIsLockAccount = false;
		if ($arWithdrawRequest['USER_ID'] && (! ($bIsLockAccount = User::lockAccount($arWithdrawRequest['USER_ID'])) || ! User::AccountPay($arWithdrawRequest['USER_ID'], - $arWithdrawRequest['SUM'], User::ACCOUNT_TRANSACTION_TYPE_WITHDRAW_RETURN, array(
			'OBJECT_ID' => $arWithdrawRequest['WITHDRAW_OBJECT_ID']
		))))
		{
			if ($bIsLockAccount) User::unlockAccount($arWithdrawRequest['USER_ID']);
			
			$obConnection->rollbackTransaction();
			
			return $arResult;
		}
		
		if ($bIsLockAccount) User::unlockAccount($arWithdrawRequest['USER_ID']);
		
		$obConnection->query('update `b_sssr_cashback_withdraw_request` set `IS_CURRENT`=0,`IS_SUCCESS`=0,`ERROR_CODE`="' . $obSqlHelper->forSql($sErrorCode) . '",`ERROR_DESCRIPTION`="' . $obSqlHelper->forSql($sErrorDescription) . '" where `ID`=' . $iWithdrawRequestId);
		
		$iWithdrawIBlockId = \COption::GetOptionInt(Core::MODULE_ID, 'withdraw_iblock_id');
		$arStatusXmlIds = Tools::getIBlockPropertyEnumXmlIds('STATUS', $iWithdrawIBlockId);
		
		\CIBlockElement::SetPropertyValuesEx($arWithdrawRequest['WITHDRAW_OBJECT_ID'], $iWithdrawIBlockId, array(
			'DENY_EDIT' => 0,
			'STATUS' => $arStatusXmlIds['REJECTED']
		));
		
		$obConnection->commitTransaction();
		
		$arResult['IS_SUCCESS'] = true;
		
		return $arResult;
	}
}
